/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.splitter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a physical plan split into engine-specific execution segments.
 *
 * <p>A split plan contains:
 * <ul>
 *   <li>A list of execution segments (one per engine boundary)</li>
 *   <li>An execution graph showing dependencies between segments</li>
 * </ul>
 *
 * <p>The execution graph determines the order in which segments must be
 * executed. Segments with no dependencies can execute first, followed by
 * segments that depend on their results.
 *
 * <p><b>Example:</b>
 * <pre>
 * SplitPlan with 2 segments:
 *   Segment 1 (Lucene): Filter + IndexScan
 *   Segment 2 (DataFusion): Aggregate
 *   Execution Graph: Segment 1 → Segment 2
 * </pre>
 */
public class SplitPlan {

    private final List<ExecutionSegment> segments;
    private final ExecutionGraph executionGraph;

    /**
     * Constructs a new SplitPlan.
     *
     * @param segments the execution segments
     * @param executionGraph the execution graph showing dependencies
     */
    public SplitPlan(List<ExecutionSegment> segments, ExecutionGraph executionGraph) {
        this.segments = segments != null ? new ArrayList<>(segments) : new ArrayList<>();
        this.executionGraph = Objects.requireNonNull(executionGraph, "executionGraph cannot be null");
    }

    /**
     * Gets the execution segments.
     *
     * @return unmodifiable list of execution segments
     */
    public List<ExecutionSegment> getSegments() {
        return Collections.unmodifiableList(segments);
    }

    /**
     * Gets the execution graph.
     *
     * @return the execution graph
     */
    public ExecutionGraph getExecutionGraph() {
        return executionGraph;
    }

    /**
     * Gets the number of segments in this split plan.
     *
     * @return the number of segments
     */
    public int getSegmentCount() {
        return segments.size();
    }

    /**
     * Checks if this plan has multiple segments (hybrid execution).
     *
     * @return true if there are multiple segments, false otherwise
     */
    public boolean isHybrid() {
        return segments.size() > 1;
    }

    @Override
    public String toString() {
        return String.format(
            "SplitPlan[segments=%d, hybrid=%b]",
            segments.size(),
            isHybrid()
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SplitPlan that = (SplitPlan) obj;
        return Objects.equals(segments, that.segments)
            && Objects.equals(executionGraph, that.executionGraph);
    }

    @Override
    public int hashCode() {
        return Objects.hash(segments, executionGraph);
    }
}
