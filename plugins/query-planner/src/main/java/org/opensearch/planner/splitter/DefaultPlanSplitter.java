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
import org.opensearch.planner.physical.operators.PhysicalOperator;
import org.opensearch.planner.physical.operators.TransferOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default implementation of the PlanSplitter interface.
 *
 * <p>This implementation splits physical plans by:
 * <ol>
 *   <li>Traversing the operator tree to identify engine boundaries</li>
 *   <li>Grouping consecutive same-engine operators into segments</li>
 *   <li>Creating execution segments with unique IDs</li>
 *   <li>Building an execution graph showing dependencies</li>
 * </ol>
 *
 * <p><b>Algorithm:</b>
 * <pre>
 * 1. Start at root operator
 * 2. Traverse tree depth-first
 * 3. When engine changes, create new segment
 * 4. Group consecutive same-engine operators
 * 5. Build dependency graph based on data flow
 * </pre>
 *
 * <p><b>Example:</b>
 * <pre>
 * Input:
 *   DataFusion: Aggregate
 *     Transfer(Lucene → DataFusion)
 *       Lucene: Filter
 *         Lucene: IndexScan
 *
 * Output:
 *   Segment 1 (Lucene): Filter → IndexScan
 *   Segment 2 (DataFusion): Aggregate
 *   Graph: Segment 1 → Segment 2
 * </pre>
 */
public class DefaultPlanSplitter implements PlanSplitter {

    private final AtomicInteger segmentIdCounter = new AtomicInteger(0);

    /**
     * Constructs a new DefaultPlanSplitter.
     */
    public DefaultPlanSplitter() {
        // Default constructor
    }

    @Override
    public SplitPlan splitPlan(PhysicalPlan physicalPlan) {
        if (physicalPlan == null) {
            throw new SplitException("Physical plan cannot be null");
        }

        PhysicalOperator root = physicalPlan.getRoot();
        if (root == null) {
            throw new SplitException("Physical plan root cannot be null");
        }

        // Reset segment counter for each plan
        segmentIdCounter.set(0);

        // Build segments and execution graph
        List<ExecutionSegment> segments = new ArrayList<>();
        ExecutionGraph executionGraph = new ExecutionGraph();
        Map<PhysicalOperator, String> operatorToSegment = new HashMap<>();

        // Split the plan starting from root
        splitOperatorTree(root, null, segments, executionGraph, operatorToSegment);

        return new SplitPlan(segments, executionGraph);
    }

    /**
     * Recursively splits the operator tree into segments.
     *
     * @param operator the current operator
     * @param parentSegmentId the parent segment ID (null for root)
     * @param segments the list of segments being built
     * @param executionGraph the execution graph being built
     * @param operatorToSegment map from operator to its segment ID
     * @return the segment ID containing this operator
     */
    private String splitOperatorTree(
        PhysicalOperator operator,
        String parentSegmentId,
        List<ExecutionSegment> segments,
        ExecutionGraph executionGraph,
        Map<PhysicalOperator, String> operatorToSegment
    ) {
        // Check if we've already processed this operator
        if (operatorToSegment.containsKey(operator)) {
            return operatorToSegment.get(operator);
        }

        ExecutionEngine currentEngine = operator.getExecutionEngine();

        // Handle Transfer operators specially - they mark segment boundaries
        if (operator instanceof TransferOperator) {
            // Process child (source of transfer)
            if (operator.getChildren().isEmpty()) {
                throw new SplitException("Transfer operator must have a child");
            }
            
            PhysicalOperator child = operator.getChildren().get(0);
            String childSegmentId = splitOperatorTree(
                child,
                null,  // Force new segment after transfer
                segments,
                executionGraph,
                operatorToSegment
            );

            // The transfer operator itself doesn't belong to a segment
            // It just marks the boundary between segments
            return childSegmentId;
        }

        // Process children first (bottom-up)
        List<String> childSegmentIds = new ArrayList<>();
        for (PhysicalOperator child : operator.getChildren()) {
            String childSegmentId = splitOperatorTree(
                child,
                null,  // Don't pass parent segment to children yet
                segments,
                executionGraph,
                operatorToSegment
            );
            if (!childSegmentIds.contains(childSegmentId)) {
                childSegmentIds.add(childSegmentId);
            }
        }

        // Determine which segment this operator belongs to
        String currentSegmentId = null;
        
        // Check if we can reuse an existing child segment
        if (childSegmentIds.size() == 1) {
            // Single child - check if we can add to its segment
            String childSegmentId = childSegmentIds.get(0);
            ExecutionSegment childSegment = findSegmentById(segments, childSegmentId);
            
            if (childSegment != null && childSegment.getEngine() == currentEngine) {
                // Same engine as child - reuse the segment
                currentSegmentId = childSegmentId;
            }
        }
        
        // If we couldn't reuse a segment, create a new one
        if (currentSegmentId == null) {
            currentSegmentId = generateSegmentId();
            
            // Create segment with this operator as root
            ExecutionSegment segment = new ExecutionSegment(
                currentSegmentId,
                currentEngine,
                operator,
                childSegmentIds
            );
            
            segments.add(segment);
            executionGraph.addSegment(currentSegmentId);

            // Add dependencies in execution graph
            for (String childSegmentId : childSegmentIds) {
                executionGraph.addDependency(childSegmentId, currentSegmentId);
            }
        }

        // Record this operator's segment
        operatorToSegment.put(operator, currentSegmentId);

        return currentSegmentId;
    }

    /**
     * Finds a segment by its ID.
     *
     * @param segments the list of segments
     * @param segmentId the segment ID to find
     * @return the segment, or null if not found
     */
    private ExecutionSegment findSegmentById(List<ExecutionSegment> segments, String segmentId) {
        for (ExecutionSegment segment : segments) {
            if (segment.getSegmentId().equals(segmentId)) {
                return segment;
            }
        }
        return null;
    }

    /**
     * Generates a unique segment ID.
     *
     * @return a unique segment ID
     */
    private String generateSegmentId() {
        return "segment-" + segmentIdCounter.incrementAndGet();
    }
}
