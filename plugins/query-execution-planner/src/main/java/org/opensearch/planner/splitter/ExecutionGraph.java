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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

/**
 * Represents a directed acyclic graph (DAG) of execution segment dependencies.
 *
 * <p>The execution graph determines the order in which segments must be
 * executed. It ensures that:
 * <ul>
 *   <li>Segments with no dependencies execute first</li>
 *   <li>Segments execute only after their dependencies complete</li>
 *   <li>The graph is acyclic (no circular dependencies)</li>
 * </ul>
 *
 * <p><b>Example:</b>
 * <pre>
 * Segment 1 (Lucene) → Segment 2 (DataFusion) → Segment 3 (DataFusion)
 *                   ↘ Segment 4 (DataFusion) ↗
 *
 * Execution order: [Segment 1], [Segment 2, Segment 4], [Segment 3]
 * </pre>
 */
public class ExecutionGraph {

    private final Map<String, Set<String>> adjacencyList;
    private final Map<String, Integer> inDegree;

    /**
     * Constructs a new ExecutionGraph.
     */
    public ExecutionGraph() {
        this.adjacencyList = new HashMap<>();
        this.inDegree = new HashMap<>();
    }

    /**
     * Adds a segment to the graph.
     *
     * @param segmentId the segment ID
     */
    public void addSegment(String segmentId) {
        Objects.requireNonNull(segmentId, "segmentId cannot be null");
        adjacencyList.putIfAbsent(segmentId, new HashSet<>());
        inDegree.putIfAbsent(segmentId, 0);
    }

    /**
     * Adds a dependency edge from one segment to another.
     *
     * <p>This indicates that {@code toSegment} depends on {@code fromSegment},
     * meaning {@code fromSegment} must complete before {@code toSegment} can execute.
     *
     * @param fromSegment the segment that must complete first
     * @param toSegment the segment that depends on fromSegment
     */
    public void addDependency(String fromSegment, String toSegment) {
        Objects.requireNonNull(fromSegment, "fromSegment cannot be null");
        Objects.requireNonNull(toSegment, "toSegment cannot be null");

        // Ensure both segments exist
        addSegment(fromSegment);
        addSegment(toSegment);

        // Add edge
        if (adjacencyList.get(fromSegment).add(toSegment)) {
            // Only increment in-degree if this is a new edge
            inDegree.put(toSegment, inDegree.get(toSegment) + 1);
        }
    }

    /**
     * Gets the segments that have no dependencies (can execute first).
     *
     * @return list of segment IDs with no dependencies
     */
    public List<String> getRootSegments() {
        List<String> roots = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                roots.add(entry.getKey());
            }
        }
        return roots;
    }

    /**
     * Gets the segments that depend on the given segment.
     *
     * @param segmentId the segment ID
     * @return list of dependent segment IDs
     */
    public List<String> getDependents(String segmentId) {
        Set<String> dependents = adjacencyList.get(segmentId);
        return dependents != null ? new ArrayList<>(dependents) : Collections.emptyList();
    }

    /**
     * Performs a topological sort to determine execution order.
     *
     * <p>Returns a list of segment IDs in the order they should be executed.
     * Segments at the same level (no dependencies between them) can be
     * executed in parallel.
     *
     * @return list of segment IDs in topological order
     * @throws IllegalStateException if the graph contains a cycle
     */
    public List<String> topologicalSort() {
        List<String> result = new ArrayList<>();
        Map<String, Integer> tempInDegree = new HashMap<>(inDegree);
        Queue<String> queue = new LinkedList<>();

        // Add all segments with no dependencies
        for (Map.Entry<String, Integer> entry : tempInDegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.offer(entry.getKey());
            }
        }

        // Process segments in topological order
        while (!queue.isEmpty()) {
            String current = queue.poll();
            result.add(current);

            // Reduce in-degree for all dependents
            for (String dependent : adjacencyList.get(current)) {
                int newInDegree = tempInDegree.get(dependent) - 1;
                tempInDegree.put(dependent, newInDegree);

                if (newInDegree == 0) {
                    queue.offer(dependent);
                }
            }
        }

        // Check for cycles
        if (result.size() != adjacencyList.size()) {
            throw new IllegalStateException("Execution graph contains a cycle");
        }

        return result;
    }

    /**
     * Gets the number of segments in the graph.
     *
     * @return the number of segments
     */
    public int size() {
        return adjacencyList.size();
    }

    /**
     * Checks if the graph is empty.
     *
     * @return true if empty, false otherwise
     */
    public boolean isEmpty() {
        return adjacencyList.isEmpty();
    }

    @Override
    public String toString() {
        return String.format("ExecutionGraph[segments=%d]", adjacencyList.size());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ExecutionGraph that = (ExecutionGraph) obj;
        return Objects.equals(adjacencyList, that.adjacencyList)
            && Objects.equals(inDegree, that.inDegree);
    }

    @Override
    public int hashCode() {
        return Objects.hash(adjacencyList, inDegree);
    }
}
