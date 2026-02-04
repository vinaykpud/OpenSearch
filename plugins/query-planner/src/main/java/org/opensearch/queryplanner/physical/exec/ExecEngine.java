/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.physical.exec;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * Engine execution node - wraps a subplan to be executed by a native engine.
 *
 * <p>The ExecEngine node contains an ExecNode subtree that will be executed
 * by a native engine (e.g., DataFusion) or a Java fallback executor.
 *
 * <p>The subplan is serialized along with the ExecEngine so it can be
 * transported to data nodes. The actual conversion to engine-specific format
 * happens at execution time on the data node.
 *
 * <h2>Example:</h2>
 * <pre>
 * ExecEngine
 *   └── subPlan: ExecFilter
 *                  └── ExecProject
 *                        └── ExecScan
 *
 * At execution time:
 *   - JavaFallbackExecutor: uses existing OperatorFactory on subPlan
 *   - DataFusionExecutor: converts subPlan to DataFusion plan, executes via JNI
 * </pre>
 */
public class ExecEngine implements ExecNode {

    private final ExecNode subPlan;

    /**
     * Create an engine node wrapping a subplan.
     *
     * @param subPlan The ExecNode tree to execute in the engine
     */
    public ExecEngine(ExecNode subPlan) {
        this.subPlan = subPlan;
    }

    /**
     * Deserialize from stream.
     */
    public ExecEngine(StreamInput in) throws IOException {
        this.subPlan = ExecNode.readNode(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ExecNode.writeNode(out, subPlan);
    }

    @Override
    public NodeType getType() {
        return NodeType.ENGINE;
    }

    @Override
    public List<ExecNode> getChildren() {
        // subPlan is internal to the engine, not exposed as a child
        // The engine executes the subPlan atomically
        return List.of();
    }

    /**
     * Get the subplan to be executed by the engine.
     */
    public ExecNode getSubPlan() {
        return subPlan;
    }

    @Override
    public String toString() {
        return "ExecEngine[subPlan=" + subPlan.getType() + "]";
    }
}
