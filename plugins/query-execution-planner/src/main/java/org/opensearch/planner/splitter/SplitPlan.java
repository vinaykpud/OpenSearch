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

    /**
     * Serializes this split plan to JSON format.
     *
     * @return JSON string representation of the split plan
     */
    public String toJson() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"segmentCount\":").append(segments.size()).append(",");
        json.append("\"isHybrid\":").append(isHybrid()).append(",");
        
        // Serialize segments
        json.append("\"segments\":[");
        for (int i = 0; i < segments.size(); i++) {
            if (i > 0) {
                json.append(",");
            }
            ExecutionSegment segment = segments.get(i);
            json.append("{");
            json.append("\"segmentId\":\"").append(escapeJson(segment.getSegmentId())).append("\",");
            json.append("\"engine\":\"").append(segment.getEngine()).append("\",");
            json.append("\"hasDependencies\":").append(segment.hasDependencies()).append(",");
            json.append("\"dependencies\":[");
            List<String> deps = segment.getDependencies();
            for (int j = 0; j < deps.size(); j++) {
                if (j > 0) {
                    json.append(",");
                }
                json.append("\"").append(escapeJson(deps.get(j))).append("\"");
            }
            json.append("],");
            json.append("\"rootOperatorType\":\"").append(segment.getRoot().getOperatorType()).append("\",");
            json.append("\"rootOperatorEngine\":\"").append(segment.getRoot().getExecutionEngine()).append("\"");
            json.append("}");
        }
        json.append("],");
        
        // Serialize execution graph
        json.append("\"executionGraph\":{");
        json.append("\"size\":").append(executionGraph.size()).append(",");
        json.append("\"isEmpty\":").append(executionGraph.isEmpty()).append(",");
        json.append("\"executionOrder\":[");
        List<String> executionOrder = executionGraph.topologicalSort();
        for (int i = 0; i < executionOrder.size(); i++) {
            if (i > 0) {
                json.append(",");
            }
            json.append("\"").append(escapeJson(executionOrder.get(i))).append("\"");
        }
        json.append("],");
        json.append("\"rootSegments\":[");
        List<String> rootSegments = executionGraph.getRootSegments();
        for (int i = 0; i < rootSegments.size(); i++) {
            if (i > 0) {
                json.append(",");
            }
            json.append("\"").append(escapeJson(rootSegments.get(i))).append("\"");
        }
        json.append("]");
        json.append("}");
        
        json.append("}");
        return json.toString();
    }

    /**
     * Escapes special characters in JSON strings.
     *
     * @param str the string to escape
     * @return escaped string
     */
    private String escapeJson(String str) {
        if (str == null) {
            return "";
        }
        return str.replace("\\", "\\\\")
                  .replace("\"", "\\\"")
                  .replace("\n", "\\n")
                  .replace("\r", "\\r")
                  .replace("\t", "\\t");
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
