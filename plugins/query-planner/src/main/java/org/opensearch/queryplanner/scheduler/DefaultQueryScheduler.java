/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.scheduler;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.queryplanner.action.ShardQueryPlanAction;
import org.opensearch.queryplanner.action.ShardQueryPlanRequest;
import org.opensearch.queryplanner.action.ShardQueryPlanResponse;
import org.opensearch.queryplanner.physical.exec.ExecAggregate;
import org.opensearch.queryplanner.physical.exec.ExecEngine;
import org.opensearch.queryplanner.physical.exec.ExecNode;
import org.opensearch.queryplanner.physical.exec.ExecNativeScan;
import org.opensearch.queryplanner.physical.exec.ExecScan;
import org.opensearch.queryplanner.physical.operator.AggregateOperator;
import org.opensearch.queryplanner.physical.operator.GatheredDataOperator;
import org.opensearch.queryplanner.physical.operator.LuceneDataProvider;
import org.opensearch.queryplanner.physical.operator.Operator;
import org.opensearch.queryplanner.physical.operator.OperatorFactory;
import org.opensearch.queryplanner.physical.operator.ScanOperator;
import org.opensearch.transport.TransportService;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Default implementation of QueryScheduler.
 *
 * <p>Splits plans into stages, dispatches to shards, and executes.
 */
public class DefaultQueryScheduler implements QueryScheduler {

    private static final Logger logger = LogManager.getLogger(DefaultQueryScheduler.class);

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final StageBuilder stageBuilder;

    public DefaultQueryScheduler(
            TransportService transportService,
            ClusterService clusterService,
            IndicesService indicesService) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.stageBuilder = new StageBuilder();
    }

    @Override
    public CompletableFuture<VectorSchemaRoot> schedule(ExecNode plan, SchedulerContext context) {
        return CompletableFuture.supplyAsync(() -> {
            // Stage the plan
            String indexName = findIndexName(plan);
            StageBuilder.StagedPlan stagedPlan = stageBuilder.build(plan);

            if (!stagedPlan.hasExchange()) {
                throw new IllegalStateException("Plan must have exchange for distributed execution");
            }

            List<ShardRouting> shards = getShards(indexName);

            // Execute leaf stage on shards
            List<VectorSchemaRoot> shardResults = executeOnShards(
                stagedPlan.getLeafStage().getPlanFragment(),
                shards,
                context
            );

            // Execute root stage if present
            BufferAllocator allocator = context.getAllocator();
            if (stagedPlan.getRootStage() != null) {
                VectorSchemaRoot result = executeRootStage(
                    stagedPlan.getRootStage().getPlanFragment(),
                    shardResults,
                    allocator
                );
                closeAll(shardResults);
                return result;
            } else {
                if (shardResults.size() == 1) {
                    return shardResults.get(0);
                }
                VectorSchemaRoot result = concatenateResults(shardResults, allocator);
                closeAll(shardResults);
                return result;
            }
        });
    }

    private List<ShardRouting> getShards(String indexName) {
        List<ShardRouting> shards = new ArrayList<>();
        if (indexName == null || clusterService == null) return shards;

        IndexRoutingTable indexRouting = clusterService.state().routingTable().index(indexName);
        if (indexRouting == null) return shards;

        for (IndexShardRoutingTable shardTable : indexRouting) {
            ShardRouting primary = shardTable.primaryShard();
            if (primary != null && primary.active()) {
                shards.add(primary);
            }
        }
        return shards;
    }

    private String findIndexName(ExecNode node) {
        if (node == null) return null;
        if (node instanceof ExecScan) return ((ExecScan) node).getIndexName();
        if (node instanceof ExecNativeScan) return ((ExecNativeScan) node).getTableName();
        if (node instanceof ExecEngine) return findIndexName(((ExecEngine) node).getSubPlan());
        for (ExecNode child : node.getChildren()) {
            String name = findIndexName(child);
            if (name != null) return name;
        }
        return null;
    }

    private void closeAll(List<VectorSchemaRoot> roots) {
        for (VectorSchemaRoot root : roots) {
            if (root != null) root.close();
        }
    }

    // --- Execution logic ---

    private List<VectorSchemaRoot> executeOnShards(ExecNode leafPlan, List<ShardRouting> shards,
                                                    SchedulerContext context) {
        List<VectorSchemaRoot> results = new ArrayList<>();
        for (ShardRouting shard : shards) {
            try {
                ShardQueryPlanRequest request = new ShardQueryPlanRequest(shard.shardId(), leafPlan);
                ShardQueryPlanResponse response = executeShardRequest(shard, request, context);
                if (response.hasError()) {
                    logger.error("Shard {} error: {}", shard.shardId(), response.getErrorMessage());
                    continue;
                }
                results.add(convertResponseToVectorSchemaRoot(response, context.getAllocator()));
            } catch (Exception e) {
                logger.error("Error on shard {}", shard.shardId(), e);
            }
        }
        return results;
    }

    private ShardQueryPlanResponse executeShardRequest(ShardRouting shard, ShardQueryPlanRequest request,
                                                        SchedulerContext context) {
        if (isShardLocal(request.shardId())) {
            return executeLocalShard(request, context);
        } else {
            return executeRemoteShard(shard, request, context);
        }
    }

    private boolean isShardLocal(ShardId shardId) {
        IndexService indexService = indicesService.indexService(shardId.getIndex());
        if (indexService == null) return false;
        return indexService.getShardOrNull(shardId.id()) != null;
    }

    private ShardQueryPlanResponse executeLocalShard(ShardQueryPlanRequest request, SchedulerContext context) {
        ShardId shardId = request.shardId();
        try {
            IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
            IndexShard shard = indexService.getShard(shardId.id());
            MapperService mapperService = indexService.mapperService();

            try (Engine.Searcher searcher = shard.acquireSearcher("query-plan")) {
                LuceneDataProvider dataProvider = new LuceneDataProvider(
                    searcher, mapperService, context.getBatchSize());
                VectorSchemaRoot result = executeExecNode(request.getPlanFragment(), dataProvider, context);

                List<String> columnNames = new ArrayList<>();
                for (var field : result.getSchema().getFields()) {
                    columnNames.add(field.getName());
                }

                List<Object[]> rows = new ArrayList<>();
                for (int row = 0; row < result.getRowCount(); row++) {
                    Object[] rowData = new Object[columnNames.size()];
                    for (int col = 0; col < columnNames.size(); col++) {
                        rowData[col] = result.getVector(col).getObject(row);
                    }
                    rows.add(rowData);
                }

                result.close();
                return new ShardQueryPlanResponse(shardId, columnNames, rows);
            }
        } catch (Exception e) {
            return new ShardQueryPlanResponse(shardId, e.getMessage());
        }
    }

    private ShardQueryPlanResponse executeRemoteShard(ShardRouting shard, ShardQueryPlanRequest request,
                                                       SchedulerContext context) {
        var targetNode = clusterService.state().nodes().get(shard.currentNodeId());
        if (targetNode == null) {
            return new ShardQueryPlanResponse(shard.shardId(), "Node not found: " + shard.currentNodeId());
        }

        try {
            CompletableFuture<ShardQueryPlanResponse> future = new CompletableFuture<>();
            transportService.sendRequest(
                targetNode,
                ShardQueryPlanAction.NAME,
                request,
                new org.opensearch.transport.TransportResponseHandler<ShardQueryPlanResponse>() {
                    @Override
                    public ShardQueryPlanResponse read(org.opensearch.core.common.io.stream.StreamInput in)
                            throws java.io.IOException {
                        return new ShardQueryPlanResponse(in);
                    }

                    @Override
                    public void handleResponse(ShardQueryPlanResponse response) {
                        future.complete(response);
                    }

                    @Override
                    public void handleException(org.opensearch.transport.TransportException exp) {
                        future.completeExceptionally(exp);
                    }

                    @Override
                    public String executor() {
                        return org.opensearch.threadpool.ThreadPool.Names.SEARCH;
                    }
                }
            );
            return future.get(context.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            return new ShardQueryPlanResponse(shard.shardId(), e.getMessage());
        }
    }

    public VectorSchemaRoot executeExecNode(ExecNode execNode, ScanOperator.DataProvider dataProvider,
                                             SchedulerContext context) {
        BufferAllocator allocator = context.getAllocator();
        OperatorFactory factory = new OperatorFactory(dataProvider);
        Operator rootOp = factory.create(execNode);

        rootOp.open(allocator);
        try {
            List<VectorSchemaRoot> batches = new ArrayList<>();
            VectorSchemaRoot batch;
            while ((batch = rootOp.next()) != null) {
                batches.add(batch);
            }

            if (batches.isEmpty()) {
                return VectorSchemaRoot.create(
                    new org.apache.arrow.vector.types.pojo.Schema(List.of()),
                    allocator
                );
            }

            if (batches.size() == 1) {
                return batches.get(0);
            }

            VectorSchemaRoot merged = concatenateResults(batches, allocator);
            closeAll(batches);
            return merged;
        } finally {
            rootOp.close();
        }
    }

    private VectorSchemaRoot executeRootStage(ExecNode rootPlan, List<VectorSchemaRoot> gatheredData,
                                               BufferAllocator allocator) {
        if (rootPlan instanceof ExecAggregate) {
            ExecAggregate aggNode = (ExecAggregate) rootPlan;
            GatheredDataOperator gatheredOp = new GatheredDataOperator(gatheredData);
            AggregateOperator finalAgg = new AggregateOperator(
                gatheredOp,
                aggNode.getGroupBy(),
                aggNode.getAggregates(),
                aggNode.getMode()
            );

            finalAgg.open(allocator);
            try {
                VectorSchemaRoot result = finalAgg.next();
                if (result == null) {
                    return VectorSchemaRoot.create(
                        new org.apache.arrow.vector.types.pojo.Schema(List.of()),
                        allocator
                    );
                }
                return result;
            } finally {
                finalAgg.close();
            }
        }
        return concatenateResults(gatheredData, allocator);
    }

    // --- Arrow conversion helpers ---

    private VectorSchemaRoot convertResponseToVectorSchemaRoot(ShardQueryPlanResponse response,
                                                                BufferAllocator allocator) {
        List<String> columnNames = response.getColumnNames();
        List<Object[]> rows = response.getRows();

        if (columnNames.isEmpty()) {
            return VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(List.of()),
                allocator
            );
        }

        List<org.apache.arrow.vector.types.pojo.Field> fields = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            Object sample = rows.isEmpty() ? null : rows.get(0)[i];
            org.apache.arrow.vector.types.pojo.ArrowType type;
            if (sample instanceof String) {
                type = org.apache.arrow.vector.types.pojo.ArrowType.Utf8.INSTANCE;
            } else if (sample instanceof Double || sample instanceof Float) {
                type = new org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint(
                    org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
            } else if (sample instanceof Long || sample instanceof Integer) {
                type = new org.apache.arrow.vector.types.pojo.ArrowType.Int(64, true);
            } else {
                type = org.apache.arrow.vector.types.pojo.ArrowType.Utf8.INSTANCE;
            }
            fields.add(new org.apache.arrow.vector.types.pojo.Field(
                columnNames.get(i),
                org.apache.arrow.vector.types.pojo.FieldType.nullable(type),
                null
            ));
        }

        VectorSchemaRoot root = VectorSchemaRoot.create(
            new org.apache.arrow.vector.types.pojo.Schema(fields),
            allocator
        );

        for (org.apache.arrow.vector.FieldVector vec : root.getFieldVectors()) {
            vec.allocateNew();
        }

        for (int row = 0; row < rows.size(); row++) {
            Object[] rowData = rows.get(row);
            for (int col = 0; col < columnNames.size(); col++) {
                Object value = rowData[col];
                org.apache.arrow.vector.FieldVector vec = root.getVector(col);
                if (value == null) continue;
                if (vec instanceof org.apache.arrow.vector.VarCharVector) {
                    ((org.apache.arrow.vector.VarCharVector) vec).setSafe(row,
                        value.toString().getBytes(StandardCharsets.UTF_8));
                } else if (vec instanceof org.apache.arrow.vector.Float8Vector) {
                    ((org.apache.arrow.vector.Float8Vector) vec).setSafe(row, ((Number) value).doubleValue());
                } else if (vec instanceof org.apache.arrow.vector.BigIntVector) {
                    ((org.apache.arrow.vector.BigIntVector) vec).setSafe(row, ((Number) value).longValue());
                }
            }
        }

        root.setRowCount(rows.size());
        return root;
    }

    private VectorSchemaRoot concatenateResults(List<VectorSchemaRoot> batches, BufferAllocator allocator) {
        if (batches.isEmpty()) {
            return VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(List.of()),
                allocator
            );
        }
        if (batches.size() == 1) {
            return batches.get(0);
        }

        org.apache.arrow.vector.types.pojo.Schema schema = batches.get(0).getSchema();
        VectorSchemaRoot result = VectorSchemaRoot.create(schema, allocator);

        for (org.apache.arrow.vector.FieldVector vec : result.getFieldVectors()) {
            vec.allocateNew();
        }

        int currentRow = 0;
        for (VectorSchemaRoot batch : batches) {
            for (int row = 0; row < batch.getRowCount(); row++) {
                for (int col = 0; col < schema.getFields().size(); col++) {
                    copyValue(batch.getVector(col), row, result.getVector(col), currentRow);
                }
                currentRow++;
            }
        }

        result.setRowCount(currentRow);
        return result;
    }

    private void copyValue(org.apache.arrow.vector.FieldVector src, int srcRow,
                           org.apache.arrow.vector.FieldVector dst, int dstRow) {
        if (src.isNull(srcRow)) return;

        if (src instanceof org.apache.arrow.vector.VarCharVector) {
            ((org.apache.arrow.vector.VarCharVector) dst).setSafe(dstRow,
                ((org.apache.arrow.vector.VarCharVector) src).get(srcRow));
        } else if (src instanceof org.apache.arrow.vector.Float8Vector) {
            ((org.apache.arrow.vector.Float8Vector) dst).setSafe(dstRow,
                ((org.apache.arrow.vector.Float8Vector) src).get(srcRow));
        } else if (src instanceof org.apache.arrow.vector.BigIntVector) {
            ((org.apache.arrow.vector.BigIntVector) dst).setSafe(dstRow,
                ((org.apache.arrow.vector.BigIntVector) src).get(srcRow));
        } else if (src instanceof org.apache.arrow.vector.IntVector) {
            ((org.apache.arrow.vector.IntVector) dst).setSafe(dstRow,
                ((org.apache.arrow.vector.IntVector) src).get(srcRow));
        }
    }
}
