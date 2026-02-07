/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.splitter;

import org.opensearch.planner.physical.PhysicalPlan;
import org.opensearch.planner.physical.operators.ExecutionEngine;
import org.opensearch.planner.physical.operators.FilterOperator;
import org.opensearch.planner.physical.operators.HashAggregateOperator;
import org.opensearch.planner.physical.operators.IndexScanOperator;
import org.opensearch.planner.physical.operators.TransferOperator;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for the Plan Splitter.
 *
 * <p>These tests verify that the plan splitter correctly:
 * <ul>
 *   <li>Identifies engine boundaries</li>
 *   <li>Groups consecutive same-engine operators</li>
 *   <li>Creates execution segments</li>
 *   <li>Builds execution graphs with correct dependencies</li>
 * </ul>
 */
public class PlanSplitterTests extends OpenSearchTestCase {

    /**
     * Test splitting a Lucene-only plan (no split needed).
     */
    public void testSplitLuceneOnlyPlan() {
        // Create a Lucene-only plan: Filter → IndexScan
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

        // Verify: Should have 1 segment (all Lucene)
        assertEquals(1, splitPlan.getSegmentCount());
        assertFalse(splitPlan.isHybrid());

        ExecutionSegment segment = splitPlan.getSegments().get(0);
        assertEquals(ExecutionEngine.LUCENE, segment.getEngine());
        assertEquals(0, segment.getDependencies().size());
        assertFalse(segment.hasDependencies());
    }

    /**
     * Test splitting a DataFusion-only plan (no split needed).
     */
    public void testSplitDataFusionOnlyPlan() {
        // Create a DataFusion-only plan: Aggregate
        HashAggregateOperator aggregate = new HashAggregateOperator(
            Arrays.asList("category"),
            Arrays.asList("COUNT(*)"),
            Collections.emptyList(),
            Arrays.asList("category", "count")
        );

        PhysicalPlan plan = new PhysicalPlan(aggregate);

        // Split the plan
        PlanSplitter splitter = new DefaultPlanSplitter();
        SplitPlan splitPlan = splitter.splitPlan(plan);

        // Verify: Should have 1 segment (all DataFusion)
        assertEquals(1, splitPlan.getSegmentCount());
        assertFalse(splitPlan.isHybrid());

        ExecutionSegment segment = splitPlan.getSegments().get(0);
        assertEquals(ExecutionEngine.DATAFUSION, segment.getEngine());
        assertEquals(0, segment.getDependencies().size());
    }

    /**
     * Test splitting a hybrid plan (Lucene → DataFusion).
     */
    public void testSplitHybridPlan() {
        // Create a hybrid plan:
        // DataFusion: Aggregate
        //   Transfer(Lucene → DataFusion)
        //     Lucene: Filter
        //       Lucene: IndexScan

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

        // Verify: Should have 2 segments (Lucene + DataFusion)
        assertEquals(2, splitPlan.getSegmentCount());
        assertTrue(splitPlan.isHybrid());

        // Find Lucene and DataFusion segments
        ExecutionSegment luceneSegment = null;
        ExecutionSegment dataFusionSegment = null;

        for (ExecutionSegment segment : splitPlan.getSegments()) {
            if (segment.getEngine() == ExecutionEngine.LUCENE) {
                luceneSegment = segment;
            } else if (segment.getEngine() == ExecutionEngine.DATAFUSION) {
                dataFusionSegment = segment;
            }
        }

        assertNotNull("Lucene segment should exist", luceneSegment);
        assertNotNull("DataFusion segment should exist", dataFusionSegment);

        // Verify Lucene segment has no dependencies
        assertEquals(0, luceneSegment.getDependencies().size());
        assertFalse(luceneSegment.hasDependencies());

        // Verify DataFusion segment depends on Lucene segment
        assertEquals(1, dataFusionSegment.getDependencies().size());
        assertTrue(dataFusionSegment.hasDependencies());
        assertEquals(luceneSegment.getSegmentId(), dataFusionSegment.getDependencies().get(0));
    }

    /**
     * Test execution graph topological sort.
     */
    public void testExecutionGraphTopologicalSort() {
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

        // Get topological sort
        List<String> executionOrder = splitPlan.getExecutionGraph().topologicalSort();

        // Verify: Should have 2 segments in order
        assertEquals(2, executionOrder.size());

        // Find which segment is Lucene and which is DataFusion
        String luceneSegmentId = null;
        String dataFusionSegmentId = null;

        for (ExecutionSegment segment : splitPlan.getSegments()) {
            if (segment.getEngine() == ExecutionEngine.LUCENE) {
                luceneSegmentId = segment.getSegmentId();
            } else if (segment.getEngine() == ExecutionEngine.DATAFUSION) {
                dataFusionSegmentId = segment.getSegmentId();
            }
        }

        assertNotNull(luceneSegmentId);
        assertNotNull(dataFusionSegmentId);

        // Verify Lucene segment comes before DataFusion segment
        int luceneIndex = executionOrder.indexOf(luceneSegmentId);
        int dataFusionIndex = executionOrder.indexOf(dataFusionSegmentId);
        assertTrue("Lucene segment should execute before DataFusion segment", luceneIndex < dataFusionIndex);
    }

    /**
     * Test segment grouping (consecutive same-engine operators).
     */
    public void testSegmentGrouping() {
        // Create a plan with multiple consecutive Lucene operators:
        // Lucene: Filter2
        //   Lucene: Filter1
        //     Lucene: IndexScan

        IndexScanOperator indexScan = new IndexScanOperator(
            "test-index",
            Arrays.asList("field1", "field2")
        );

        FilterOperator filter1 = new FilterOperator(
            "field1 > 100",
            ExecutionEngine.LUCENE,
            Collections.singletonList(indexScan),
            Arrays.asList("field1", "field2")
        );

        FilterOperator filter2 = new FilterOperator(
            "field2 < 200",
            ExecutionEngine.LUCENE,
            Collections.singletonList(filter1),
            Arrays.asList("field1", "field2")
        );

        PhysicalPlan plan = new PhysicalPlan(filter2);

        // Split the plan
        PlanSplitter splitter = new DefaultPlanSplitter();
        SplitPlan splitPlan = splitter.splitPlan(plan);

        // Verify: Should have 1 segment (all consecutive Lucene operators grouped)
        assertEquals(1, splitPlan.getSegmentCount());
        
        ExecutionSegment segment = splitPlan.getSegments().get(0);
        assertEquals(ExecutionEngine.LUCENE, segment.getEngine());
    }

    /**
     * Test execution graph with root segments.
     */
    public void testExecutionGraphRootSegments() {
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

        // Get root segments (segments with no dependencies)
        List<String> rootSegments = splitPlan.getExecutionGraph().getRootSegments();

        // Verify: Should have 1 root segment (Lucene)
        assertEquals(1, rootSegments.size());

        // Find the Lucene segment
        ExecutionSegment luceneSegment = null;
        for (ExecutionSegment segment : splitPlan.getSegments()) {
            if (segment.getEngine() == ExecutionEngine.LUCENE) {
                luceneSegment = segment;
                break;
            }
        }

        assertNotNull(luceneSegment);
        assertEquals(luceneSegment.getSegmentId(), rootSegments.get(0));
    }

    /**
     * Test null plan throws exception.
     */
    public void testNullPlanThrowsException() {
        PlanSplitter splitter = new DefaultPlanSplitter();
        
        expectThrows(SplitException.class, () -> {
            splitter.splitPlan(null);
        });
    }

    /**
     * Test execution graph size and isEmpty.
     */
    public void testExecutionGraphSizeAndEmpty() {
        ExecutionGraph graph = new ExecutionGraph();
        
        assertTrue(graph.isEmpty());
        assertEquals(0, graph.size());

        graph.addSegment("segment-1");
        assertFalse(graph.isEmpty());
        assertEquals(1, graph.size());

        graph.addSegment("segment-2");
        assertEquals(2, graph.size());
    }

    /**
     * Test execution graph dependencies.
     */
    public void testExecutionGraphDependencies() {
        ExecutionGraph graph = new ExecutionGraph();
        
        graph.addSegment("segment-1");
        graph.addSegment("segment-2");
        graph.addDependency("segment-1", "segment-2");

        List<String> dependents = graph.getDependents("segment-1");
        assertEquals(1, dependents.size());
        assertEquals("segment-2", dependents.get(0));

        List<String> noDependents = graph.getDependents("segment-2");
        assertEquals(0, noDependents.size());
    }
}
