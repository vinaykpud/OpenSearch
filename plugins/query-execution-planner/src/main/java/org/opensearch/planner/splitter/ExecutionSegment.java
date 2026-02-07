/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.splitter;

import org.opensearch.planner.physical.operators.ExecutionEngine;
import org.opensearch.planner.physical.operators.PhysicalOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a segment of a physical plan that executes on a single engine.
 *
 * <p>An execution segment contains:
 * <ul>
 *   <li>A unique segment ID</li>
 *   <li>The execution engine (Lucene or DataFusion)</li>
 *   <li>The root operator of the segment's operator tree</li>
 *   <li>Dependencies on other segments (segments that must complete first)</li>
 * </ul>
 *
 * <p>Segments are the unit of execution in the split plan. Each segment
 * executes independently on its assigned engine, consuming input from
 * dependent segments and producing output for dependent segments.
 *
 * <p><b>Example:</b>
 * <pre>
 * Segment ID: "segment-1"
 * Engine: LUCENE
 * Root: Filter(price > 100) → IndexScan(products)
 * Dependencies: []
 * </pre>
 */
public class ExecutionSegment {

    private final String segmentId;
    private final ExecutionEngine engine;
    private final PhysicalOperator root;
    private final List<String> dependencies;

    /**
     * Constructs a new ExecutionSegment.
     *
     * @param segmentId unique identifier for this segment
     * @param engine the execution engine for this segment
     * @param root the root operator of this segment's operator tree
     * @param dependencies IDs of segments that must complete before this one
     */
    public ExecutionSegment(
        String segmentId,
        ExecutionEngine engine,
        PhysicalOperator root,
        List<String> dependencies
    ) {
        this.segmentId = Objects.requireNonNull(segmentId, "segmentId cannot be null");
        this.engine = Objects.requireNonNull(engine, "engine cannot be null");
        this.root = Objects.requireNonNull(root, "root cannot be null");
        this.dependencies = dependencies != null ? new ArrayList<>(dependencies) : new ArrayList<>();
    }

    /**
     * Gets the segment ID.
     *
     * @return the segment ID
     */
    public String getSegmentId() {
        return segmentId;
    }

    /**
     * Gets the execution engine for this segment.
     *
     * @return the execution engine
     */
    public ExecutionEngine getEngine() {
        return engine;
    }

    /**
     * Gets the root operator of this segment's operator tree.
     *
     * @return the root operator
     */
    public PhysicalOperator getRoot() {
        return root;
    }

    /**
     * Gets the IDs of segments that must complete before this segment.
     *
     * @return unmodifiable list of dependency segment IDs
     */
    public List<String> getDependencies() {
        return Collections.unmodifiableList(dependencies);
    }

    /**
     * Checks if this segment has dependencies.
     *
     * @return true if there are dependencies, false otherwise
     */
    public boolean hasDependencies() {
        return !dependencies.isEmpty();
    }

    @Override
    public String toString() {
        return String.format(
            "ExecutionSegment[id=%s, engine=%s, dependencies=%s]",
            segmentId,
            engine,
            dependencies
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
        ExecutionSegment that = (ExecutionSegment) obj;
        return Objects.equals(segmentId, that.segmentId)
            && engine == that.engine
            && Objects.equals(root, that.root)
            && Objects.equals(dependencies, that.dependencies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(segmentId, engine, root, dependencies);
    }
}
