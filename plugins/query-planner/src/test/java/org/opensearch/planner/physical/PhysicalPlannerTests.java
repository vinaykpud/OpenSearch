/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.planner.physical.operators.ExecutionEngine;
import org.opensearch.planner.physical.operators.FilterOperator;
import org.opensearch.planner.physical.operators.HashAggregateOperator;
import org.opensearch.planner.physical.operators.HashJoinOperator;
import org.opensearch.planner.physical.operators.IndexScanOperator;
import org.opensearch.planner.physical.operators.OperatorType;
import org.opensearch.planner.physical.operators.PhysicalOperator;
import org.opensearch.planner.physical.operators.ProjectOperator;
import org.opensearch.planner.physical.operators.SortOperator;
import org.opensearch.planner.physical.operators.TransferOperator;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for physical planner.
 *
 * <p>These tests verify that the physical planner correctly converts
 * logical plans to physical plans with proper engine assignments and
 * Transfer operator insertion.
 */
public class PhysicalPlannerTests extends OpenSearchTestCase {

    private PhysicalPlanner planner;
    private RelBuilder relBuilder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        planner = new DefaultPhysicalPlanner();

        // Create test schema with a products table
        SchemaPlus testSchema = Frameworks.createRootSchema(false);
        
        testSchema.add(
            "products",
            new org.apache.calcite.schema.impl.AbstractTable() {
                @Override
                public org.apache.calcite.rel.type.RelDataType getRowType(org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
                    return typeFactory.builder()
                        .add("category", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
                        .add("price", org.apache.calcite.sql.type.SqlTypeName.INTEGER)
                        .add("name", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
                        .build();
                }
            }
        );

        relBuilder = RelBuilder.create(
            Frameworks.newConfigBuilder()
                .defaultSchema(testSchema)
                .build()
        );
    }

    /**
     * Tests conversion of a simple table scan.
     */
    public void testTableScanConversion() throws Exception {
        RelNode logicalPlan = relBuilder.scan("products").build();
        
        PhysicalPlan physicalPlan = planner.generatePhysicalPlan(logicalPlan);
        
        assertNotNull(physicalPlan);
        PhysicalOperator root = physicalPlan.getRoot();
        assertTrue(root instanceof IndexScanOperator);
        assertEquals(ExecutionEngine.LUCENE, root.getExecutionEngine());
        assertEquals(OperatorType.INDEX_SCAN, root.getOperatorType());
        
        IndexScanOperator scan = (IndexScanOperator) root;
        assertEquals("products", scan.getIndexName());
    }

    /**
     * Tests conversion of a filter operation.
     */
    public void testFilterConversion() throws Exception {
        RelNode logicalPlan = relBuilder
            .scan("products")
            .filter(
                relBuilder.call(
                    SqlStdOperatorTable.GREATER_THAN,
                    relBuilder.field("price"),
                    relBuilder.literal(100)
                )
            )
            .build();
        
        PhysicalPlan physicalPlan = planner.generatePhysicalPlan(logicalPlan);
        
        assertNotNull(physicalPlan);
        PhysicalOperator root = physicalPlan.getRoot();
        assertTrue(root instanceof FilterOperator);
        assertEquals(ExecutionEngine.LUCENE, root.getExecutionEngine());
        
        FilterOperator filter = (FilterOperator) root;
        assertNotNull(filter.getPredicate());
        assertEquals(1, filter.getChildren().size());
        assertTrue(filter.getChildren().get(0) instanceof IndexScanOperator);
    }

    /**
     * Tests conversion of a projection operation.
     */
    public void testProjectConversion() throws Exception {
        RelNode logicalPlan = relBuilder
            .scan("products")
            .project(
                relBuilder.field("category"),
                relBuilder.field("price")
            )
            .build();
        
        PhysicalPlan physicalPlan = planner.generatePhysicalPlan(logicalPlan);
        
        assertNotNull(physicalPlan);
        PhysicalOperator root = physicalPlan.getRoot();
        assertTrue(root instanceof ProjectOperator);
        assertEquals(ExecutionEngine.LUCENE, root.getExecutionEngine());
        
        ProjectOperator project = (ProjectOperator) root;
        assertEquals(2, project.getOutputSchema().size());
        assertEquals(1, project.getChildren().size());
    }

    /**
     * Tests conversion of an aggregation operation.
     */
    public void testAggregateConversion() throws Exception {
        RelNode logicalPlan = relBuilder
            .scan("products")
            .aggregate(
                relBuilder.groupKey("category"),
                relBuilder.count(false, "count")
            )
            .build();
        
        PhysicalPlan physicalPlan = planner.generatePhysicalPlan(logicalPlan);
        
        assertNotNull(physicalPlan);
        PhysicalOperator root = physicalPlan.getRoot();
        assertTrue(root instanceof HashAggregateOperator);
        assertEquals(ExecutionEngine.DATAFUSION, root.getExecutionEngine());
        
        HashAggregateOperator agg = (HashAggregateOperator) root;
        assertEquals(1, agg.getGroupByFields().size());
        assertEquals("category", agg.getGroupByFields().get(0));
        assertEquals(1, agg.getAggregateFunctions().size());
    }

    /**
     * Tests conversion of a join operation.
     */
    public void testJoinConversion() throws Exception {
        RelNode logicalPlan = relBuilder
            .scan("products")
            .scan("products")
            .join(
                JoinRelType.INNER,
                relBuilder.equals(
                    relBuilder.field(2, 0, "category"),
                    relBuilder.field(2, 1, "category")
                )
            )
            .build();
        
        PhysicalPlan physicalPlan = planner.generatePhysicalPlan(logicalPlan);
        
        assertNotNull(physicalPlan);
        PhysicalOperator root = physicalPlan.getRoot();
        assertTrue(root instanceof HashJoinOperator);
        assertEquals(ExecutionEngine.DATAFUSION, root.getExecutionEngine());
        
        HashJoinOperator join = (HashJoinOperator) root;
        assertEquals(HashJoinOperator.JoinType.INNER, join.getJoinType());
        assertEquals(2, join.getChildren().size());
    }

    /**
     * Tests conversion of a sort operation.
     */
    public void testSortConversion() throws Exception {
        RelNode logicalPlan = relBuilder
            .scan("products")
            .sort(relBuilder.field("price"))
            .build();
        
        PhysicalPlan physicalPlan = planner.generatePhysicalPlan(logicalPlan);
        
        assertNotNull(physicalPlan);
        PhysicalOperator root = physicalPlan.getRoot();
        assertTrue(root instanceof SortOperator);
        assertEquals(ExecutionEngine.DATAFUSION, root.getExecutionEngine());
        
        SortOperator sort = (SortOperator) root;
        assertEquals(1, sort.getSortKeys().size());
        assertEquals("price", sort.getSortKeys().get(0).getFieldName());
    }

    /**
     * Tests Transfer operator insertion for mixed engine operations.
     *
     * <p>Logical plan: Aggregate(Filter(TableScan))
     * <p>Physical plan: HashAggregate[DATAFUSION] → Transfer → Filter[LUCENE] → IndexScan[LUCENE]
     */
    public void testTransferOperatorInsertion() throws Exception {
        RelNode logicalPlan = relBuilder
            .scan("products")
            .filter(
                relBuilder.call(
                    SqlStdOperatorTable.GREATER_THAN,
                    relBuilder.field("price"),
                    relBuilder.literal(100)
                )
            )
            .aggregate(
                relBuilder.groupKey("category"),
                relBuilder.count(false, "count")
            )
            .build();
        
        PhysicalPlan physicalPlan = planner.generatePhysicalPlan(logicalPlan);
        
        assertNotNull(physicalPlan);
        PhysicalOperator root = physicalPlan.getRoot();
        
        // Root should be HashAggregate (DataFusion)
        assertTrue(root instanceof HashAggregateOperator);
        assertEquals(ExecutionEngine.DATAFUSION, root.getExecutionEngine());
        
        // Child should be Transfer operator
        assertEquals(1, root.getChildren().size());
        PhysicalOperator child = root.getChildren().get(0);
        assertTrue(child instanceof TransferOperator);
        
        TransferOperator transfer = (TransferOperator) child;
        assertEquals(ExecutionEngine.LUCENE, transfer.getFromEngine());
        assertEquals(ExecutionEngine.DATAFUSION, transfer.getToEngine());
        
        // Transfer's child should be Filter (Lucene)
        assertEquals(1, transfer.getChildren().size());
        PhysicalOperator filterOp = transfer.getChildren().get(0);
        assertTrue(filterOp instanceof FilterOperator);
        assertEquals(ExecutionEngine.LUCENE, filterOp.getExecutionEngine());
    }

    /**
     * Tests that no Transfer operator is inserted when all operations use the same engine.
     */
    public void testNoTransferForSameEngine() throws Exception {
        RelNode logicalPlan = relBuilder
            .scan("products")
            .filter(
                relBuilder.call(
                    SqlStdOperatorTable.GREATER_THAN,
                    relBuilder.field("price"),
                    relBuilder.literal(100)
                )
            )
            .project(
                relBuilder.field("category"),
                relBuilder.field("price")
            )
            .build();
        
        PhysicalPlan physicalPlan = planner.generatePhysicalPlan(logicalPlan);
        
        assertNotNull(physicalPlan);
        
        // All operations should be Lucene, no Transfer operators
        PhysicalOperator root = physicalPlan.getRoot();
        assertTrue(root instanceof ProjectOperator);
        assertEquals(ExecutionEngine.LUCENE, root.getExecutionEngine());
        
        PhysicalOperator filter = root.getChildren().get(0);
        assertTrue(filter instanceof FilterOperator);
        assertEquals(ExecutionEngine.LUCENE, filter.getExecutionEngine());
        assertFalse(filter instanceof TransferOperator);
        
        PhysicalOperator scan = filter.getChildren().get(0);
        assertTrue(scan instanceof IndexScanOperator);
        assertEquals(ExecutionEngine.LUCENE, scan.getExecutionEngine());
    }

    /**
     * Tests error handling for null logical plan.
     */
    public void testNullLogicalPlan() {
        PlanningException exception = expectThrows(
            PlanningException.class,
            () -> planner.generatePhysicalPlan(null)
        );
        assertTrue(exception.getMessage().contains("cannot be null"));
    }

    /**
     * Tests error handling for null planning context.
     */
    public void testNullPlanningContext() {
        RelNode logicalPlan = relBuilder.scan("products").build();
        
        PlanningException exception = expectThrows(
            PlanningException.class,
            () -> planner.generatePhysicalPlan(logicalPlan, null)
        );
        assertTrue(exception.getMessage().contains("cannot be null"));
    }

    /**
     * Tests JSON serialization of physical plan.
     */
    public void testPhysicalPlanJsonSerialization() throws Exception {
        RelNode logicalPlan = relBuilder
            .scan("products")
            .filter(
                relBuilder.call(
                    SqlStdOperatorTable.GREATER_THAN,
                    relBuilder.field("price"),
                    relBuilder.literal(100)
                )
            )
            .build();
        
        PhysicalPlan physicalPlan = planner.generatePhysicalPlan(logicalPlan);
        
        String json = physicalPlan.toJson();
        assertNotNull(json);
        assertTrue(json.contains("PhysicalPlan"));
        assertTrue(json.contains("FILTER"));
        assertTrue(json.contains("INDEX_SCAN"));
        assertTrue(json.contains("LUCENE"));
    }

    /**
     * Tests DOT graph generation of physical plan.
     */
    public void testPhysicalPlanDotGraphGeneration() throws Exception {
        RelNode logicalPlan = relBuilder
            .scan("products")
            .filter(
                relBuilder.call(
                    SqlStdOperatorTable.GREATER_THAN,
                    relBuilder.field("price"),
                    relBuilder.literal(100)
                )
            )
            .aggregate(
                relBuilder.groupKey("category"),
                relBuilder.count(false, "count")
            )
            .build();
        
        PhysicalPlan physicalPlan = planner.generatePhysicalPlan(logicalPlan);
        
        String dot = physicalPlan.toDotGraph();
        assertNotNull(dot);
        assertTrue(dot.contains("digraph PhysicalPlan"));
        assertTrue(dot.contains("HASH_AGGREGATE"));
        assertTrue(dot.contains("TRANSFER"));
        assertTrue(dot.contains("FILTER"));
        assertTrue(dot.contains("INDEX_SCAN"));
        assertTrue(dot.contains("DATAFUSION"));
        assertTrue(dot.contains("LUCENE"));
    }

    /**
     * Tests complex query with multiple operators and engine transitions.
     */
    public void testComplexQueryWithMultipleEngineTransitions() throws Exception {
        RelNode logicalPlan = relBuilder
            .scan("products")
            .filter(
                relBuilder.call(
                    SqlStdOperatorTable.GREATER_THAN,
                    relBuilder.field("price"),
                    relBuilder.literal(100)
                )
            )
            .project(
                relBuilder.field("category"),
                relBuilder.field("price")
            )
            .aggregate(
                relBuilder.groupKey("category"),
                relBuilder.sum(false, "total_price", relBuilder.field("price"))
            )
            .sort(relBuilder.field("total_price"))
            .build();
        
        PhysicalPlan physicalPlan = planner.generatePhysicalPlan(logicalPlan);
        
        assertNotNull(physicalPlan);
        
        // Root should be Sort (DataFusion)
        PhysicalOperator root = physicalPlan.getRoot();
        assertTrue(root instanceof SortOperator);
        assertEquals(ExecutionEngine.DATAFUSION, root.getExecutionEngine());
        
        // Should have HashAggregate as child
        PhysicalOperator agg = root.getChildren().get(0);
        assertTrue(agg instanceof HashAggregateOperator);
        assertEquals(ExecutionEngine.DATAFUSION, agg.getExecutionEngine());
        
        // Should have Transfer operator before Lucene operations
        PhysicalOperator transfer = agg.getChildren().get(0);
        assertTrue(transfer instanceof TransferOperator);
        
        // Verify the plan is valid
        String json = physicalPlan.toJson();
        assertNotNull(json);
        assertTrue(json.contains("SORT"));
        assertTrue(json.contains("HASH_AGGREGATE"));
        assertTrue(json.contains("TRANSFER"));
    }
}
