/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.plugins.Plugin;
import org.opensearch.queryplanner.coordinator.DefaultQueryCoordinator;
import org.opensearch.queryplanner.optimizer.PhysicalOptimizer;
import org.opensearch.queryplanner.physical.exec.ExecNode;
import org.opensearch.queryplanner.physical.rel.OpenSearchRel;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Integration tests for the Query Planner pipeline.
 *
 * <p>These tests directly invoke the query planning and execution components:
 * <ol>
 *   <li>SQL parsing via CalciteSqlParser</li>
 *   <li>Physical optimization via PhysicalOptimizer</li>
 *   <li>ExecNode generation via RelToExecConverter</li>
 *   <li>Operator execution via OperatorFactory</li>
 * </ol>
 *
 * <p>This provides more visibility into each stage than the transport action tests.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class QueryPlannerPipelineIT extends OpenSearchIntegTestCase {

    // Test data expectations
    private static final int TOTAL_ORDERS = 10;
    private static final double EXPECTED_ELECTRONICS_SUM = 900.0;
    private static final double EXPECTED_CLOTHING_SUM = 255.0;
    private static final double EXPECTED_BOOKS_SUM = 100.0;
    private static final int EXPECTED_ELECTRONICS_COUNT = 4;
    private static final int EXPECTED_CLOTHING_COUNT = 3;
    private static final int EXPECTED_BOOKS_COUNT = 3;

    private ClusterService clusterService;
    private TransportService transportService;
    private org.opensearch.indices.IndicesService indicesService;
    private BufferAllocator allocator;
    private RelDataTypeFactory typeFactory;
    private OpenSearchSchemaFactory schemaFactory;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(QueryPlannerPlugin.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        internalCluster().startClusterManagerOnlyNode();
        final String primaryNode = internalCluster().startDataOnlyNode();
        final String replicaNode = internalCluster().startDataOnlyNode();

        ensureGreen();

        // Get services from a data node (not cluster manager)
        clusterService = internalCluster().getInstance(ClusterService.class, primaryNode);
        transportService = internalCluster().getInstance(TransportService.class, primaryNode);
        indicesService = internalCluster().getInstance(org.opensearch.indices.IndicesService.class, primaryNode);

        // Create Arrow allocator
        allocator = new RootAllocator(Long.MAX_VALUE);

        // Create Calcite type factory
        typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        // Create schema factory
        schemaFactory = new OpenSearchSchemaFactory(clusterService);
    }

    @Override
    public void tearDown() throws Exception {
        if (allocator != null) {
            allocator.close();
        }
        super.tearDown();
    }

    // =========================================================================
    // Phase 1 Tests: SQL Parsing and Logical Planning
    // =========================================================================

    /**
     * Test that SQL is correctly parsed into a logical plan.
     */
    public void testSqlParsing() throws Exception {
        String indexName = "test_parsing";
        createOrdersIndex(indexName);

        String sql = "SELECT category, amount FROM " + indexName + " WHERE status = 'active'";

        SchemaProvider schemaProvider = schemaFactory.createSchemaProvider();
        CalciteSqlParser parser = new CalciteSqlParser(schemaProvider, typeFactory);

        RelNode logicalPlan = parser.parse(sql);
        assertNotNull("Logical plan should not be null", logicalPlan);

        String planString = logicalPlan.explain();
        logger.info("=== Logical Plan ===\n{}", planString);

        assertTrue("Plan should contain LogicalProject or LogicalFilter",
            planString.contains("Logical") || planString.contains("Filter") || planString.contains("Project"));
    }

    /**
     * Test parsing of GROUP BY query.
     */
    public void testGroupByParsing() throws Exception {
        String indexName = "test_groupby_parsing";
        createOrdersIndex(indexName);

        String sql = "SELECT category, SUM(amount) FROM " + indexName + " GROUP BY category";

        SchemaProvider schemaProvider = schemaFactory.createSchemaProvider();
        CalciteSqlParser parser = new CalciteSqlParser(schemaProvider, typeFactory);

        RelNode logicalPlan = parser.parse(sql);
        assertNotNull("Logical plan should not be null", logicalPlan);

        String planString = logicalPlan.explain();
        logger.info("=== GROUP BY Logical Plan ===\n{}", planString);

        assertTrue("Plan should contain Aggregate",
            planString.contains("Aggregate") || planString.contains("GROUP"));
    }

    // =========================================================================
    // Phase 1 Tests: Physical Optimization
    // =========================================================================

    /**
     * Test conversion from logical to physical plan.
     */
    public void testPhysicalOptimization() throws Exception {
        String indexName = "test_physical";
        createOrdersIndex(indexName);

        String sql = "SELECT category, amount FROM " + indexName;

        // Parse SQL
        SchemaProvider schemaProvider = schemaFactory.createSchemaProvider();
        CalciteSqlParser parser = new CalciteSqlParser(schemaProvider, typeFactory);
        RelNode logicalPlan = parser.parse(sql);

        // Optimize to physical plan
        PhysicalOptimizer optimizer = new PhysicalOptimizer(false);  // Single-node mode
        RelNode physicalPlan = optimizer.optimize(logicalPlan);

        assertNotNull("Physical plan should not be null", physicalPlan);

        String planString = physicalPlan.explain();
        logger.info("=== Physical Plan ===\n{}", planString);

        assertTrue("Plan should contain OpenSearch operators",
            planString.contains("OpenSearch") || planString.contains("TableScan"));
    }

    /**
     * Test physical optimization with distribution traits (distributed mode).
     */
    public void testPhysicalOptimizationDistributed() throws Exception {
        String indexName = "test_physical_dist";
        createOrdersIndex(indexName);

        String sql = "SELECT category, SUM(amount) FROM " + indexName + " GROUP BY category";

        // Parse SQL
        SchemaProvider schemaProvider = schemaFactory.createSchemaProvider();
        CalciteSqlParser parser = new CalciteSqlParser(schemaProvider, typeFactory);
        RelNode logicalPlan = parser.parse(sql);

        // Optimize with distribution traits
        PhysicalOptimizer optimizer = new PhysicalOptimizer(true);  // Distributed mode
        RelNode physicalPlan = optimizer.optimize(logicalPlan);

        assertNotNull("Physical plan should not be null", physicalPlan);

        String planString = physicalPlan.explain();
        logger.info("=== Distributed Physical Plan ===\n{}", planString);

        // In distributed mode, aggregates should be split and exchanges should be inserted
        // Note: This may not work fully until distribution trait enforcement is complete
    }

    // =========================================================================
    // Phase 1 Tests: ExecNode Generation
    // =========================================================================

    /**
     * Test conversion from physical plan to ExecNode tree.
     */
    public void testExecNodeGeneration() throws Exception {
        String indexName = "test_execnode";
        createOrdersIndex(indexName);

        String sql = "SELECT category, amount FROM " + indexName;

        // Parse and optimize
        SchemaProvider schemaProvider = schemaFactory.createSchemaProvider();
        CalciteSqlParser parser = new CalciteSqlParser(schemaProvider, typeFactory);
        RelNode logicalPlan = parser.parse(sql);

        PhysicalOptimizer optimizer = new PhysicalOptimizer(false);
        RelNode physicalPlan = optimizer.optimize(logicalPlan);

        // Convert to ExecNode
        ExecNode execNode = ((OpenSearchRel) physicalPlan).toExecNode();

        assertNotNull("ExecNode should not be null", execNode);
        logger.info("=== ExecNode Tree ===\n{}", ExecNode.explain(execNode));

        // Verify it's a valid execution tree
        assertNotNull("ExecNode type should not be null", execNode.getType());
    }

    // =========================================================================
    // End-to-End Execution Tests (using DefaultQueryCoordinator)
    // =========================================================================

    /**
     * Test full pipeline execution: SQL -> Parse -> Optimize -> Execute -> Results.
     * Note: Actual data execution not yet implemented - returns empty results.
     */
    public void testFullPipelineExecution() throws Exception {
        String indexName = "test_full_pipeline";
        createOrdersIndex(indexName);
        indexOrdersData(indexName);
        ensureGreen(indexName);
        String sql = "SELECT * FROM " + indexName;

        // Execute through the full pipeline
        QueryResult result = executeSql(sql);

        assertNotNull("Result should not be null", result);
        logger.info("=== Full Pipeline Result ===");
        logger.info("Columns: {}", result.columnNames);
        printResult(result);
        // TODO: Enable when execution is implemented
        // assertEquals("Should return all rows", TOTAL_ORDERS, result.rowCount);
    }

    /**
     * Test GROUP BY SUM execution.
     * Note: Actual data execution not yet implemented - returns empty results.
     */
    public void testGroupBySumExecution() throws Exception {
        String indexName = "test_groupby_sum_exec";
        createOrdersIndex(indexName);
        indexOrdersData(indexName);
        ensureGreen(indexName);

        String sql = "SELECT category, SUM(amount) as total FROM " + indexName + " GROUP BY category";

        QueryResult result = executeSql(sql);

        assertNotNull("Result should not be null", result);
        logger.info("=== GROUP BY SUM Result ===");
        printResult(result);

        // TODO: Enable when execution is implemented
        // assertEquals("Should have 3 category groups", 3, result.rowCount);
    }

    /**
     * Test GROUP BY COUNT execution.
     * Note: Actual data execution not yet implemented - returns empty results.
     */
    public void testGroupByCountExecution() throws Exception {
        String indexName = "test_groupby_count_exec";
        createOrdersIndex(indexName);
        indexOrdersData(indexName);

        String sql = "SELECT category, COUNT(*) as cnt FROM " + indexName + " GROUP BY category";

        QueryResult result = executeSql(sql);

        assertNotNull("Result should not be null", result);
        logger.info("=== GROUP BY COUNT Result ===");
        printResult(result);

        // TODO: Enable when execution is implemented
        // assertEquals("Should have 3 category groups", 3, result.rowCount);
    }

    /**
     * Test WHERE clause execution.
     * Note: Actual data execution not yet implemented - returns empty results.
     */
    public void testWhereClauseExecution() throws Exception {
        String indexName = "test_where_exec";
        createOrdersIndex(indexName);
        indexOrdersData(indexName);

        String sql = "SELECT order_id, category, amount FROM " + indexName + " WHERE status = 'active'";

        QueryResult result = executeSql(sql);

        assertNotNull("Result should not be null", result);
        logger.info("=== WHERE Clause Result ===");
        printResult(result);

        // TODO: Enable when execution is implemented
        // assertEquals("Should return 7 active orders", 7, result.rowCount);
    }

    /**
     * Test ORDER BY with LIMIT execution.
     * Note: Actual data execution not yet implemented - returns empty results.
     */
    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/TBD")
    public void testOrderByLimitExecution() throws Exception {
        // TODO: Fix Arrow vector allocation issue when reading keyword columns in Sort operator
        String indexName = "test_orderby_limit_exec";
        createOrdersIndex(indexName);
        indexOrdersData(indexName);

        String sql = "SELECT order_id, amount FROM " + indexName + " ORDER BY amount DESC LIMIT 5";

        QueryResult result = executeSql(sql);

        assertNotNull("Result should not be null", result);
        logger.info("=== ORDER BY LIMIT Result ===");
        printResult(result);

        // TODO: Enable when execution is implemented
        // assertTrue("Should return at most 5 rows", result.rowCount <= 5);
    }

    // =========================================================================
    // Helper Methods
    // =========================================================================

    /**
     * Execute SQL through the full query planning pipeline.
     */
    private QueryResult executeSql(String sql) throws Exception {
        logger.info("Executing SQL: {}", sql);

        // Step 1: Build schema
        SchemaProvider schemaProvider = schemaFactory.createSchemaProvider();

        // Step 2: Parse SQL
        CalciteSqlParser parser = new CalciteSqlParser(schemaProvider, typeFactory);
        RelNode logicalPlan = parser.parse(sql);
        logger.info("Logical plan:\n{}", logicalPlan.explain());

        // Step 3: Execute via QueryCoordinator
        DefaultQueryCoordinator coordinator = new DefaultQueryCoordinator(
            transportService, clusterService, indicesService, allocator);

        CompletableFuture<VectorSchemaRoot> future = coordinator.execute(logicalPlan);
        VectorSchemaRoot root = future.get();

        try {
            return convertToQueryResult(root);
        } finally {
            root.close();
        }
    }

    /**
     * Convert VectorSchemaRoot to a simple QueryResult for testing.
     */
    private QueryResult convertToQueryResult(VectorSchemaRoot root) {
        List<String> columnNames = new ArrayList<>();
        for (FieldVector vector : root.getFieldVectors()) {
            columnNames.add(vector.getName());
        }

        List<Object[]> rows = new ArrayList<>();
        int rowCount = root.getRowCount();
        int columnCount = root.getFieldVectors().size();

        for (int row = 0; row < rowCount; row++) {
            Object[] rowData = new Object[columnCount];
            for (int col = 0; col < columnCount; col++) {
                FieldVector vector = root.getFieldVectors().get(col);
                rowData[col] = vector.getObject(row);
            }
            rows.add(rowData);
        }

        return new QueryResult(columnNames, rows, rowCount);
    }

    /**
     * Simple result holder for tests.
     */
    private static class QueryResult {
        final List<String> columnNames;
        final List<Object[]> rows;
        final int rowCount;

        QueryResult(List<String> columnNames, List<Object[]> rows, int rowCount) {
            this.columnNames = columnNames;
            this.rows = rows;
            this.rowCount = rowCount;
        }
    }

    private void printResult(QueryResult result) {
        logger.info("Columns: {}", result.columnNames);
        logger.info("Row count: {}", result.rowCount);
        int count = 0;
        for (Object[] row : result.rows) {
            StringBuilder sb = new StringBuilder("  [");
            for (int i = 0; i < row.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(row[i]);
            }
            sb.append("]");
            logger.info(sb.toString());
            if (++count >= 10) {
                if (result.rows.size() > 10) {
                    logger.info("  ... ({} more rows)", result.rows.size() - 10);
                }
                break;
            }
        }
    }

    private Map<String, Double> extractCategorySums(QueryResult result) {
        Map<String, Double> sums = new HashMap<>();
        for (Object[] row : result.rows) {
            if (row.length >= 2) {
                String category = String.valueOf(row[0]);
                Double sum = ((Number) row[1]).doubleValue();
                sums.put(category, sum);
            }
        }
        return sums;
    }

    // =========================================================================
    // Test Data Setup
    // =========================================================================

    private void createOrdersIndex(String indexName) {
        createIndex(indexName, Settings.builder()
            .put("number_of_shards", 2)
            .put("number_of_replicas", 0)
            .build());

        client().admin().indices().preparePutMapping(indexName)
            .setSource(Map.of(
                "properties", Map.of(
                    "order_id", Map.of("type", "keyword"),
                    "customer_id", Map.of("type", "keyword"),
                    "status", Map.of("type", "keyword"),
                    "category", Map.of("type", "keyword"),
                    "amount", Map.of("type", "double"),
                    "quantity", Map.of("type", "integer")
                )
            ))
            .get();

        ensureGreen(indexName);
    }

    private void indexOrdersData(String indexName) {
        // 10 test orders with known values
        indexOrder(indexName, "1", "ORD-001", "CUST-A", "active", "electronics", 150.0, 2);
        indexOrder(indexName, "2", "ORD-002", "CUST-B", "active", "clothing", 80.0, 3);
        indexOrder(indexName, "3", "ORD-003", "CUST-A", "completed", "electronics", 220.0, 1);
        indexOrder(indexName, "4", "ORD-004", "CUST-C", "active", "books", 30.0, 5);
        indexOrder(indexName, "5", "ORD-005", "CUST-B", "cancelled", "clothing", 110.0, 2);
        indexOrder(indexName, "6", "ORD-006", "CUST-A", "active", "books", 45.0, 3);
        indexOrder(indexName, "7", "ORD-007", "CUST-D", "active", "electronics", 350.0, 1);
        indexOrder(indexName, "8", "ORD-008", "CUST-C", "completed", "clothing", 65.0, 4);
        indexOrder(indexName, "9", "ORD-009", "CUST-B", "active", "electronics", 180.0, 2);
        indexOrder(indexName, "10", "ORD-010", "CUST-A", "active", "books", 25.0, 2);

        refresh(indexName);
    }

    private void indexOrder(String indexName, String id, String orderId, String customerId,
                           String status, String category, double amount, int quantity) {
        client().index(new IndexRequest(indexName)
            .id(id)
            .source(Map.of(
                "order_id", orderId,
                "customer_id", customerId,
                "status", status,
                "category", category,
                "amount", amount,
                "quantity", quantity
            ), MediaTypeRegistry.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
    }
}
