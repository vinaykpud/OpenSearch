/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical;

import org.opensearch.planner.physical.operators.ExecutionEngine;
import org.opensearch.planner.physical.operators.FilterOperator;
import org.opensearch.planner.physical.operators.HashAggregateOperator;
import org.opensearch.planner.physical.operators.HashJoinOperator;
import org.opensearch.planner.physical.operators.IndexScanOperator;
import org.opensearch.planner.physical.operators.LimitOperator;
import org.opensearch.planner.physical.operators.OperatorType;
import org.opensearch.planner.physical.operators.PhysicalOperator;
import org.opensearch.planner.physical.operators.ProjectOperator;
import org.opensearch.planner.physical.operators.SortOperator;
import org.opensearch.planner.physical.operators.TransferOperator;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests for physical plan data model.
 *
 * <p>These tests verify:
 * <ul>
 *   <li>Operator creation and tree building</li>
 *   <li>JSON serialization</li>
 *   <li>DOT graph generation</li>
 * </ul>
 */
public class PhysicalPlanTests extends OpenSearchTestCase {

    /**
     * Tests creating a simple physical plan with IndexScan and Filter operators.
     */
    public void testSimplePhysicalPlan() {
        // Create a simple plan: Filter → IndexScan
        List<String> schema = Arrays.asList("name", "category", "price");
        
        IndexScanOperator scan = new IndexScanOperator("products", schema);
        assertEquals("products", scan.getIndexName());
        assertEquals(OperatorType.INDEX_SCAN, scan.getOperatorType());
        assertEquals(ExecutionEngine.LUCENE, scan.getExecutionEngine());
        assertEquals(schema, scan.getOutputSchema());
        assertTrue(scan.getChildren().isEmpty());
        
        FilterOperator filter = new FilterOperator(
            "price > 100",
            ExecutionEngine.LUCENE,
            Collections.singletonList(scan),
            schema
        );
        assertEquals("price > 100", filter.getPredicate());
        assertEquals(OperatorType.FILTER, filter.getOperatorType());
        assertEquals(ExecutionEngine.LUCENE, filter.getExecutionEngine());
        assertEquals(1, filter.getChildren().size());
        assertEquals(scan, filter.getChildren().get(0));
        
        PhysicalPlan plan = new PhysicalPlan(filter);
        assertEquals(filter, plan.getRoot());
    }

    /**
     * Tests creating a complex physical plan with aggregation.
     */
    public void testComplexPhysicalPlan() {
        // Create plan: HashAggregate → Transfer → Filter → IndexScan
        List<String> scanSchema = Arrays.asList("name", "category", "price");
        List<String> aggSchema = Arrays.asList("category", "count", "total_price");
        
        IndexScanOperator scan = new IndexScanOperator("products", scanSchema);
        
        FilterOperator filter = new FilterOperator(
            "price > 100",
            ExecutionEngine.LUCENE,
            Collections.singletonList(scan),
            scanSchema
        );
        
        TransferOperator transfer = new TransferOperator(
            ExecutionEngine.LUCENE,
            ExecutionEngine.DATAFUSION,
            Collections.singletonList(filter),
            scanSchema
        );
        assertEquals(ExecutionEngine.LUCENE, transfer.getFromEngine());
        assertEquals(ExecutionEngine.DATAFUSION, transfer.getToEngine());
        assertEquals(ExecutionEngine.HYBRID, transfer.getExecutionEngine());
        
        HashAggregateOperator agg = new HashAggregateOperator(
            Collections.singletonList("category"),
            Arrays.asList("COUNT(*)", "SUM(price)"),
            Collections.singletonList(transfer),
            aggSchema
        );
        assertEquals(Collections.singletonList("category"), agg.getGroupByFields());
        assertEquals(Arrays.asList("COUNT(*)", "SUM(price)"), agg.getAggregateFunctions());
        assertEquals(ExecutionEngine.DATAFUSION, agg.getExecutionEngine());
        
        PhysicalPlan plan = new PhysicalPlan(agg);
        assertNotNull(plan);
    }

    /**
     * Tests JSON serialization of physical plans.
     */
    public void testJsonSerialization() {
        List<String> schema = Arrays.asList("name", "price");
        IndexScanOperator scan = new IndexScanOperator("products", schema);
        PhysicalPlan plan = new PhysicalPlan(scan);
        
        String json = plan.toJson();
        assertNotNull(json);
        assertTrue(json.contains("PhysicalPlan"));
        assertTrue(json.contains("INDEX_SCAN"));
        assertTrue(json.contains("LUCENE"));
        assertTrue(json.contains("products"));
    }

    /**
     * Tests DOT graph generation for visualization.
     */
    public void testDotGraphGeneration() {
        List<String> schema = Arrays.asList("name", "category", "price");
        
        IndexScanOperator scan = new IndexScanOperator("products", schema);
        FilterOperator filter = new FilterOperator(
            "price > 100",
            ExecutionEngine.LUCENE,
            Collections.singletonList(scan),
            schema
        );
        
        PhysicalPlan plan = new PhysicalPlan(filter);
        String dot = plan.toDotGraph();
        
        assertNotNull(dot);
        assertTrue(dot.startsWith("digraph PhysicalPlan"));
        assertTrue(dot.contains("INDEX_SCAN"));
        assertTrue(dot.contains("FILTER"));
        assertTrue(dot.contains("LUCENE"));
        assertTrue(dot.contains("products"));
        assertTrue(dot.contains("price > 100"));
        assertTrue(dot.contains("->"));  // Edge between nodes
    }

    /**
     * Tests HashJoinOperator creation.
     */
    public void testHashJoinOperator() {
        List<String> leftSchema = Arrays.asList("id", "name");
        List<String> rightSchema = Arrays.asList("id", "category");
        List<String> joinSchema = Arrays.asList("id", "name", "category");
        
        IndexScanOperator leftScan = new IndexScanOperator("products", leftSchema);
        IndexScanOperator rightScan = new IndexScanOperator("categories", rightSchema);
        
        HashJoinOperator join = new HashJoinOperator(
            HashJoinOperator.JoinType.INNER,
            "id",
            "id",
            Arrays.asList(leftScan, rightScan),
            joinSchema
        );
        
        assertEquals(HashJoinOperator.JoinType.INNER, join.getJoinType());
        assertEquals("id", join.getLeftKey());
        assertEquals("id", join.getRightKey());
        assertEquals(2, join.getChildren().size());
        assertEquals(ExecutionEngine.DATAFUSION, join.getExecutionEngine());
    }

    /**
     * Tests SortOperator creation.
     */
    public void testSortOperator() {
        List<String> schema = Arrays.asList("name", "price");
        IndexScanOperator scan = new IndexScanOperator("products", schema);
        
        List<SortOperator.SortKey> sortKeys = Arrays.asList(
            new SortOperator.SortKey("price", false),  // DESC
            new SortOperator.SortKey("name", true)     // ASC
        );
        
        SortOperator sort = new SortOperator(
            sortKeys,
            Collections.singletonList(scan),
            schema
        );
        
        assertEquals(2, sort.getSortKeys().size());
        assertEquals("price", sort.getSortKeys().get(0).getFieldName());
        assertFalse(sort.getSortKeys().get(0).isAscending());
        assertEquals("name", sort.getSortKeys().get(1).getFieldName());
        assertTrue(sort.getSortKeys().get(1).isAscending());
    }

    /**
     * Tests LimitOperator creation.
     */
    public void testLimitOperator() {
        List<String> schema = Arrays.asList("name", "price");
        IndexScanOperator scan = new IndexScanOperator("products", schema);
        
        LimitOperator limit = new LimitOperator(
            10,
            5,
            ExecutionEngine.DATAFUSION,
            Collections.singletonList(scan),
            schema
        );
        
        assertEquals(10, limit.getLimit());
        assertEquals(5, limit.getOffset());
        assertEquals(ExecutionEngine.DATAFUSION, limit.getExecutionEngine());
    }

    /**
     * Tests that TransferOperator requires different engines.
     */
    public void testTransferOperatorRequiresDifferentEngines() {
        List<String> schema = Arrays.asList("name", "price");
        IndexScanOperator scan = new IndexScanOperator("products", schema);
        
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new TransferOperator(
                ExecutionEngine.LUCENE,
                ExecutionEngine.LUCENE,  // Same as fromEngine
                Collections.singletonList(scan),
                schema
            )
        );
        
        assertTrue(exception.getMessage().contains("must be different"));
    }

    /**
     * Tests that HashJoinOperator requires exactly 2 children.
     */
    public void testHashJoinOperatorRequiresTwoChildren() {
        List<String> schema = Arrays.asList("id", "name");
        IndexScanOperator scan = new IndexScanOperator("products", schema);
        
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new HashJoinOperator(
                HashJoinOperator.JoinType.INNER,
                "id",
                "id",
                Collections.singletonList(scan),  // Only 1 child
                schema
            )
        );
        
        assertTrue(exception.getMessage().contains("exactly 2 children"));
    }

    /**
     * Tests ProjectOperator creation.
     */
    public void testProjectOperator() {
        List<String> fullSchema = Arrays.asList("name", "category", "price");
        List<String> projectedSchema = Arrays.asList("name", "price");
        
        IndexScanOperator scan = new IndexScanOperator("products", fullSchema);
        
        ProjectOperator project = new ProjectOperator(
            ExecutionEngine.LUCENE,
            Collections.singletonList(scan),
            projectedSchema
        );
        
        assertEquals(OperatorType.PROJECT, project.getOperatorType());
        assertEquals(ExecutionEngine.LUCENE, project.getExecutionEngine());
        assertEquals(projectedSchema, project.getOutputSchema());
        assertEquals(1, project.getChildren().size());
    }
}
