/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.engine.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.queryplanner.engine.NativeEngineExecutor;
import org.opensearch.queryplanner.physical.exec.ExecAggregate;
import org.opensearch.queryplanner.physical.exec.ExecFilter;
import org.opensearch.queryplanner.physical.exec.ExecLimit;
import org.opensearch.queryplanner.physical.exec.ExecNativeScan;
import org.opensearch.queryplanner.physical.exec.ExecNode;
import org.opensearch.queryplanner.physical.exec.ExecProject;
import org.opensearch.queryplanner.physical.exec.ExecScan;
import org.opensearch.queryplanner.physical.exec.ExecSort;
import org.opensearch.queryplanner.physical.operator.ScanOperator;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * DataFusion-based executor that uses JNI to execute queries.
 *
 * <p>This executor walks an ExecNode tree and translates operations to
 * DataFusion DataFrame operations via the JNI bridge. All operations
 * are lazy until the final collect() call.
 *
 * <h2>Execution Flow:</h2>
 * <ol>
 *   <li>Create DataFusion context</li>
 *   <li>Scan table (native engine determines how to read data)</li>
 *   <li>Apply operations from ExecNode tree (filter, project, aggregate, etc.)</li>
 *   <li>Collect results as Arrow IPC bytes</li>
 *   <li>Deserialize to VectorSchemaRoot</li>
 * </ol>
 */
public class DataFusionExecutor implements NativeEngineExecutor {

    private static final Logger logger = LogManager.getLogger(DataFusionExecutor.class);

    /**
     * Create a DataFusionExecutor.
     */
    public DataFusionExecutor() {
        // Ensure native library is loaded
        DataFusionBridge.loadLibrary();
    }

    @Override
    public VectorSchemaRoot execute(ExecNode subPlan, ScanOperator.DataProvider dataProvider,
                                     BufferAllocator allocator) {
        logger.debug("Executing via DataFusion: {}", subPlan.getType());

        long ctx = DataFusionBridge.createContext();
        long df = 0;
        try {
            // Find the scan node to get table name
            ExecNode scanNode = findScanNode(subPlan);
            String tableName = getTableName(scanNode);

            // Scan the table - native engine determines how to read data
            logger.info("Scanning table: {}", tableName);

            // TODO: This would interact with our EngineSearcher
            df = DataFusionBridge.scan(ctx, tableName);

            // Apply operations from the ExecNode tree (skip the scan, already handled)
            df = applyOperations(df, subPlan, scanNode);

            // Execute and collect results
            logger.debug("Collecting results from DataFusion");
            byte[] ipcBytes = DataFusionBridge.collect(df);

            // Deserialize Arrow IPC to VectorSchemaRoot
            VectorSchemaRoot result = deserializeArrowIpc(ipcBytes, allocator);
            logger.debug("DataFusion execution complete: {} rows", result.getRowCount());
            return result;

        } catch (Exception e) {
            logger.error("DataFusion execution failed", e);
            throw new RuntimeException("DataFusion execution failed: " + e.getMessage(), e);
        } finally {
            if (df != 0) {
                DataFusionBridge.freeDataFrame(df);
            }
            DataFusionBridge.freeContext(ctx);
        }
    }

    /**
     * Get the table name from a scan node.
     */
    private String getTableName(ExecNode scanNode) {
        if (scanNode instanceof ExecScan) {
            return ((ExecScan) scanNode).getIndexName();
        } else if (scanNode instanceof ExecNativeScan) {
            return ((ExecNativeScan) scanNode).getTableName();
        }
        throw new IllegalStateException("Unknown scan type: " + scanNode.getClass().getName());
    }

    /**
     * Find the scan node at the bottom of the ExecNode tree.
     */
    private ExecNode findScanNode(ExecNode node) {
        if (node instanceof ExecScan || node instanceof ExecNativeScan) {
            return node;
        }
        List<ExecNode> children = node.getChildren();
        if (children.isEmpty()) {
            throw new IllegalStateException("No scan node found in plan");
        }
        return findScanNode(children.get(0));
    }

    /**
     * Read data from DataProvider and serialize to Arrow IPC.
     */
    private byte[] readFromDataProvider(ScanOperator.DataProvider dataProvider, ExecScan scan,
                                        BufferAllocator allocator) throws IOException {
        if (dataProvider == null) {
            throw new IllegalStateException("DataProvider is required for Lucene scans");
        }

        dataProvider.open(scan.getIndexName(), scan.getColumns(), scan.getFilter());
        List<VectorSchemaRoot> batches = new ArrayList<>();
        try {
            VectorSchemaRoot batch;
            while ((batch = dataProvider.nextBatch(allocator)) != null) {
                batches.add(batch);
            }

            if (batches.isEmpty()) {
                throw new IllegalStateException("No data from DataProvider");
            }

            // Serialize batches to Arrow IPC
            return serializeToArrowIpc(batches);
        } finally {
            // Close all batches after serialization
            for (VectorSchemaRoot batch : batches) {
                batch.close();
            }
            dataProvider.close();
        }
    }

    /**
     * Apply operations from the ExecNode tree to the DataFrame.
     *
     * <p>This walks the tree top-down (reverse of execution order) and applies
     * operations bottom-up by using recursion.
     */
    private long applyOperations(long df, ExecNode node, ExecNode scanNode) {
        // Base case: we've reached the scan node (already handled)
        if (node == scanNode) {
            return df;
        }

        // Recursively process children first
        List<ExecNode> children = node.getChildren();
        if (!children.isEmpty()) {
            df = applyOperations(df, children.get(0), scanNode);
        }

        // Apply this node's operation
        return applyNode(df, node);
    }

    /**
     * Apply a single ExecNode operation to the DataFrame.
     */
    private long applyNode(long df, ExecNode node) {
        switch (node.getType()) {
            case FILTER:
                return applyFilter(df, (ExecFilter) node);
            case PROJECT:
                return applyProject(df, (ExecProject) node);
            case AGGREGATE:
                return applyAggregate(df, (ExecAggregate) node);
            case SORT:
                return applySort(df, (ExecSort) node);
            case LIMIT:
                return applyLimit(df, (ExecLimit) node);
            case SCAN:
            case NATIVE_SCAN:
                // Scans are handled at the start
                return df;
            default:
                logger.warn("Unsupported node type for DataFusion: {}", node.getType());
                return df;
        }
    }

    private long applyFilter(long df, ExecFilter filter) {
        String filterExpr = filter.getCondition();
        logger.debug("Applying filter: {}", filterExpr);
        long newDf = DataFusionBridge.filter(df, filterExpr);
        DataFusionBridge.freeDataFrame(df);
        return newDf;
    }

    private long applyProject(long df, ExecProject project) {
        List<String> columns = project.getColumns();
        logger.debug("Applying project: {}", columns);
        String[] colArray = columns.toArray(new String[0]);
        long newDf = DataFusionBridge.project(df, colArray);
        DataFusionBridge.freeDataFrame(df);
        return newDf;
    }

    private long applyAggregate(long df, ExecAggregate aggregate) {
        List<String> groupBy = aggregate.getGroupBy();
        List<String> aggExprs = aggregate.getAggregates();

        logger.debug("Applying aggregate: groupBy={}, aggs={}", groupBy, aggExprs);

        // Parse aggregate expressions (e.g., "SUM(amount) AS total")
        List<String> aggFuncs = new ArrayList<>();
        List<String> aggCols = new ArrayList<>();

        for (String expr : aggExprs) {
            // Simple parsing: "FUNC(col) AS alias" -> FUNC, col
            String func = expr.substring(0, expr.indexOf('(')).trim().toUpperCase();
            String col = expr.substring(expr.indexOf('(') + 1, expr.indexOf(')')).trim();
            aggFuncs.add(func);
            aggCols.add(col);
        }

        String[] groupByArray = groupBy.toArray(new String[0]);
        String[] funcArray = aggFuncs.toArray(new String[0]);
        String[] colArray = aggCols.toArray(new String[0]);

        long newDf = DataFusionBridge.aggregate(df, groupByArray, funcArray, colArray);
        DataFusionBridge.freeDataFrame(df);
        return newDf;
    }

    private long applySort(long df, ExecSort sort) {
        List<String> columns = sort.getSortColumns();
        List<Boolean> ascending = sort.getAscending();

        logger.debug("Applying sort: columns={}, ascending={}", columns, ascending);

        String[] colArray = columns.toArray(new String[0]);
        boolean[] ascArray = new boolean[ascending.size()];
        for (int i = 0; i < ascending.size(); i++) {
            ascArray[i] = ascending.get(i);
        }

        long newDf = DataFusionBridge.sort(df, colArray, ascArray);
        DataFusionBridge.freeDataFrame(df);
        return newDf;
    }

    private long applyLimit(long df, ExecLimit limit) {
        int count = limit.getLimit();
        logger.debug("Applying limit: {}", count);
        long newDf = DataFusionBridge.limit(df, count);
        DataFusionBridge.freeDataFrame(df);
        return newDf;
    }

    /**
     * Deserialize Arrow IPC bytes to VectorSchemaRoot.
     */
    private VectorSchemaRoot deserializeArrowIpc(byte[] ipcBytes, BufferAllocator allocator) throws IOException {
        if (ipcBytes == null || ipcBytes.length == 0) {
            logger.warn("Empty Arrow IPC bytes received");
            return VectorSchemaRoot.create(new org.apache.arrow.vector.types.pojo.Schema(List.of()), allocator);
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(ipcBytes);
             ArrowStreamReader reader = new ArrowStreamReader(bais, allocator)) {

            VectorSchemaRoot root = reader.getVectorSchemaRoot();

            // Read all batches and concatenate
            List<VectorSchemaRoot> batches = new ArrayList<>();
            while (reader.loadNextBatch()) {
                // Copy the batch using VectorUnloader/VectorLoader for proper deep copy
                // This is necessary because the reader reuses the same root
                VectorSchemaRoot copy = VectorSchemaRoot.create(root.getSchema(), allocator);
                org.apache.arrow.vector.ipc.message.ArrowRecordBatch arb =
                    new org.apache.arrow.vector.VectorUnloader(root).getRecordBatch();
                try {
                    new org.apache.arrow.vector.VectorLoader(copy).load(arb);
                } finally {
                    arb.close();
                }
                batches.add(copy);
            }

            logger.debug("Deserialized {} batches", batches.size());

            if (batches.isEmpty()) {
                return VectorSchemaRoot.create(root.getSchema(), allocator);
            }

            if (batches.size() == 1) {
                return batches.get(0);
            }

            // Multiple batches - concatenate them into a single VectorSchemaRoot
            int totalRows = batches.stream().mapToInt(VectorSchemaRoot::getRowCount).sum();
            logger.debug("Concatenating {} batches into {} total rows", batches.size(), totalRows);

            VectorSchemaRoot result = VectorSchemaRoot.create(root.getSchema(), allocator);
            result.setRowCount(totalRows);

            int currentRow = 0;
            for (VectorSchemaRoot batch : batches) {
                for (int col = 0; col < batch.getFieldVectors().size(); col++) {
                    org.apache.arrow.vector.FieldVector srcVec = batch.getVector(col);
                    org.apache.arrow.vector.FieldVector dstVec = result.getVector(col);
                    for (int row = 0; row < batch.getRowCount(); row++) {
                        dstVec.copyFromSafe(row, currentRow + row, srcVec);
                    }
                }
                currentRow += batch.getRowCount();
                batch.close();
            }

            return result;
        }
    }

    /**
     * Serialize VectorSchemaRoot batches to Arrow IPC format.
     */
    private byte[] serializeToArrowIpc(List<VectorSchemaRoot> batches) throws IOException {
        if (batches.isEmpty()) {
            return new byte[0];
        }

        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (org.apache.arrow.vector.ipc.ArrowStreamWriter writer =
                 new org.apache.arrow.vector.ipc.ArrowStreamWriter(batches.get(0), null, baos)) {
            writer.start();
            for (VectorSchemaRoot batch : batches) {
                writer.writeBatch();
            }
            writer.end();
        }
        return baos.toByteArray();
    }
}
