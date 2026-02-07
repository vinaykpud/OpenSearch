/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.coordinator;

import org.opensearch.planner.physical.PhysicalPlan;
import org.opensearch.planner.physical.operators.ExecutionEngine;
import org.opensearch.planner.physical.operators.FilterOperator;
import org.opensearch.planner.physical.operators.HashAggregateOperator;
import org.opensearch.planner.physical.operators.IndexScanOperator;
import org.opensearch.planner.physical.operators.TransferOperator;
import org.opensearch.planner.splitter.DefaultPlanSplitter;
import org.opensearch.planner.splitter.PlanSplitter;
import org.opensearch.planner.splitter.SplitPlan;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for the Execution Coordinator.
 *
 * <p>These tests verify that the coordinator:
 * <ul>
 *   <li>Executes segments in correct dependency order</li>
 *   <li>Collects execution statistics</li>
 *   <li>Handles errors appropriately</li>
 *   <li>Validates input parameters</li>
 * </ul>
 */
public class ExecutionCoordinatorTests extends OpenSearchTestCase {

    /**
     * Test executing a Lucene-only plan.
     */
    public void testExecuteLuceneOnlyPlan() throws Exception {
        // Create a Lucene-only plan
        IndexScanOperator indexScan = new IndexScanOperator(
            "test-index",
            Arrays.asList("field1", "field2")
        );

        FilterOperator filter = new FilterOperator(
            "field1 > 100",
            ExecutionEngine.LUCENE,
            Collections.singletonList(indexScan),
            Arrays.asList("field1", "field2")
        );

        PhysicalPlan plan = new PhysicalPlan(filter);

        // Split the plan
        PlanSplitter splitter = new DefaultPlanSplitter();
        SplitPlan splitPlan = splitter.splitPlan(plan);

        // Execute
        ExecutionCoordinator coordinator = new DefaultExecutionCoordinator();
        ExecutionContext context = ExecutionContext.builder().build();
        
        QueryResult result = coordinator.execute(splitPlan, context);

        // Verify
        assertTrue("Execution should succeed", result.isSuccess());
        assertEquals(1, result.getStatistics().getSegmentsExecuted());
        assertTrue(result.getStatistics().getTotalTimeMillis() >= 0);
    }

    /**
     * Test executing a hybrid plan (Lucene → DataFusion).
     */
    public void testExecuteHybridPlan() throws Exception {
        // Create a hybrid plan
        IndexScanOperator indexScan = new IndexScanOperator(
            "test-index",
            Arrays.asList("category", "price")
        );

        FilterOperator filter = new FilterOperator(
            "price > 100",
            ExecutionEngine.LUCENE,
            Collections.singletonList(indexScan),
            Arrays.asList("category", "price")
        );

        TransferOperator transfer = new TransferOperator(
            ExecutionEngine.LUCENE,
            ExecutionEngine.DATAFUSION,
            Collections.singletonList(filter),
            Arrays.asList("category", "price")
        );

        HashAggregateOperator aggregate = new HashAggregateOperator(
            Arrays.asList("category"),
            Arrays.asList("COUNT(*)"),
            Collections.singletonList(transfer),
            Arrays.asList("category", "count")
        );

        PhysicalPlan plan = new PhysicalPlan(aggregate);

        // Split the plan
        PlanSplitter splitter = new DefaultPlanSplitter();
        SplitPlan splitPlan = splitter.splitPlan(plan);

        // Execute
        ExecutionCoordinator coordinator = new DefaultExecutionCoordinator();
        ExecutionContext context = ExecutionContext.builder().build();
        
        QueryResult result = coordinator.execute(splitPlan, context);

        // Verify
        assertTrue("Execution should succeed", result.isSuccess());
        assertEquals(2, result.getStatistics().getSegmentsExecuted());
        assertTrue(result.getStatistics().getTotalTimeMillis() >= 0);
        assertTrue(result.getStatistics().getDocumentsProcessed() > 0);
        assertTrue(result.getStatistics().getRowsProduced() > 0);
    }

    /**
     * Test execution order follows topological sort.
     */
    public void testExecutionOrder() throws Exception {
        // Create a hybrid plan
        IndexScanOperator indexScan = new IndexScanOperator(
            "test-index",
            Arrays.asList("field1")
        );

        TransferOperator transfer = new TransferOperator(
            ExecutionEngine.LUCENE,
            ExecutionEngine.DATAFUSION,
            Collections.singletonList(indexScan),
            Arrays.asList("field1")
        );

        HashAggregateOperator aggregate = new HashAggregateOperator(
            Arrays.asList("field1"),
            Arrays.asList("COUNT(*)"),
            Collections.singletonList(transfer),
            Arrays.asList("field1", "count")
        );

        PhysicalPlan plan = new PhysicalPlan(aggregate);

        // Split the plan
        PlanSplitter splitter = new DefaultPlanSplitter();
        SplitPlan splitPlan = splitter.splitPlan(plan);

        // Get execution order
        List<String> executionOrder = splitPlan.getExecutionGraph().topologicalSort();

        // Verify Lucene segment comes before DataFusion segment
        String luceneSegmentId = null;
        String dataFusionSegmentId = null;

        for (int i = 0; i < splitPlan.getSegments().size(); i++) {
            if (splitPlan.getSegments().get(i).getEngine() == ExecutionEngine.LUCENE) {
                luceneSegmentId = splitPlan.getSegments().get(i).getSegmentId();
            } else if (splitPlan.getSegments().get(i).getEngine() == ExecutionEngine.DATAFUSION) {
                dataFusionSegmentId = splitPlan.getSegments().get(i).getSegmentId();
            }
        }

        assertNotNull(luceneSegmentId);
        assertNotNull(dataFusionSegmentId);

        int luceneIndex = executionOrder.indexOf(luceneSegmentId);
        int dataFusionIndex = executionOrder.indexOf(dataFusionSegmentId);

        assertTrue("Lucene segment should execute before DataFusion segment", luceneIndex < dataFusionIndex);
    }

    /**
     * Test null split plan throws exception.
     */
    public void testNullSplitPlanThrowsException() {
        ExecutionCoordinator coordinator = new DefaultExecutionCoordinator();
        ExecutionContext context = ExecutionContext.builder().build();

        expectThrows(CoordinationException.class, () -> {
            coordinator.execute(null, context);
        });
    }

    /**
     * Test null context throws exception.
     */
    public void testNullContextThrowsException() throws Exception {
        // Create a simple plan
        IndexScanOperator indexScan = new IndexScanOperator(
            "test-index",
            Arrays.asList("field1")
        );

        PhysicalPlan plan = new PhysicalPlan(indexScan);
        PlanSplitter splitter = new DefaultPlanSplitter();
        SplitPlan splitPlan = splitter.splitPlan(plan);

        ExecutionCoordinator coordinator = new DefaultExecutionCoordinator();

        expectThrows(CoordinationException.class, () -> {
            coordinator.execute(splitPlan, null);
        });
    }

    /**
     * Test execution statistics are collected.
     */
    public void testExecutionStatisticsCollection() throws Exception {
        // Create a hybrid plan
        IndexScanOperator indexScan = new IndexScanOperator(
            "test-index",
            Arrays.asList("field1")
        );

        TransferOperator transfer = new TransferOperator(
            ExecutionEngine.LUCENE,
            ExecutionEngine.DATAFUSION,
            Collections.singletonList(indexScan),
            Arrays.asList("field1")
        );

        HashAggregateOperator aggregate = new HashAggregateOperator(
            Arrays.asList("field1"),
            Arrays.asList("COUNT(*)"),
            Collections.singletonList(transfer),
            Arrays.asList("field1", "count")
        );

        PhysicalPlan plan = new PhysicalPlan(aggregate);

        // Split and execute
        PlanSplitter splitter = new DefaultPlanSplitter();
        SplitPlan splitPlan = splitter.splitPlan(plan);

        ExecutionCoordinator coordinator = new DefaultExecutionCoordinator();
        ExecutionContext context = ExecutionContext.builder().build();
        
        QueryResult result = coordinator.execute(splitPlan, context);

        // Verify statistics
        ExecutionStatistics stats = result.getStatistics();
        assertNotNull(stats);
        assertEquals(2, stats.getSegmentsExecuted());
        assertTrue(stats.getTotalTimeMillis() >= 0);
        assertTrue(stats.getDocumentsProcessed() >= 0);
        assertTrue(stats.getRowsProduced() >= 0);
    }

    /**
     * Test execution context builder.
     */
    public void testExecutionContextBuilder() {
        ExecutionContext context = ExecutionContext.builder()
            .withMetadata("key1", "value1")
            .withMetadata("key2", 123)
            .build();

        assertEquals("value1", context.getMetadata("key1"));
        assertEquals(123, context.getMetadata("key2"));
        assertNull(context.getMetadata("nonexistent"));
    }

    /**
     * Test execution statistics builder.
     */
    public void testExecutionStatisticsBuilder() {
        ExecutionStatistics stats = ExecutionStatistics.builder()
            .withTotalTimeMillis(100)
            .withSegmentsExecuted(2)
            .withDocumentsProcessed(1000)
            .withRowsProduced(50)
            .withSegmentTime("segment-1", 60)
            .withSegmentTime("segment-2", 40)
            .build();

        assertEquals(100, stats.getTotalTimeMillis());
        assertEquals(2, stats.getSegmentsExecuted());
        assertEquals(1000, stats.getDocumentsProcessed());
        assertEquals(50, stats.getRowsProduced());
    }
}
