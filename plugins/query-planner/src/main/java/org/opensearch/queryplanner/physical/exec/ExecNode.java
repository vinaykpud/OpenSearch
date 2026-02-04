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
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.List;

/**
 * Base interface for execution plan nodes.
 *
 * <p>ExecNodes form a tree representing the physical execution plan.
 * They are serializable via Writeable for transport to data nodes.
 */
public interface ExecNode extends Writeable {

    /**
     * Get the node type for serialization.
     */
    NodeType getType();

    /**
     * Get child nodes.
     */
    List<ExecNode> getChildren();

    /**
     * Node types for serialization registry.
     */
    enum NodeType {
        SCAN,
        FILTER,
        PROJECT,
        AGGREGATE,
        LIMIT,
        EXCHANGE,
        SORT,
        ENGINE,
        NATIVE_SCAN;

        public static NodeType fromOrdinal(int ordinal) {
            return values()[ordinal];
        }
    }

    /**
     * Write an ExecNode to stream (with type prefix).
     */
    static void writeNode(StreamOutput out, ExecNode node) throws IOException {
        out.writeVInt(node.getType().ordinal());
        node.writeTo(out);
    }

    /**
     * Read an ExecNode from stream.
     */
    static ExecNode readNode(StreamInput in) throws IOException {
        NodeType type = NodeType.fromOrdinal(in.readVInt());
        switch (type) {
            case SCAN:
                return new ExecScan(in);
            case FILTER:
                return new ExecFilter(in);
            case PROJECT:
                return new ExecProject(in);
            case AGGREGATE:
                return new ExecAggregate(in);
            case LIMIT:
                return new ExecLimit(in);
            case EXCHANGE:
                return new ExecExchange(in);
            case SORT:
                return new ExecSort(in);
            case ENGINE:
                return new ExecEngine(in);
            case NATIVE_SCAN:
                return new ExecNativeScan(in);
            default:
                throw new IllegalArgumentException("Unknown node type: " + type);
        }
    }

    /**
     * Generate a tree-formatted string representation of the execution plan.
     *
     * @param node The root node to explain
     * @return A multi-line string showing the plan tree
     */
    static String explain(ExecNode node) {
        StringBuilder sb = new StringBuilder();
        explainNode(node, sb, "", true);
        return sb.toString();
    }

    /**
     * Recursively format a node and its children.
     */
    private static void explainNode(ExecNode node, StringBuilder sb, String prefix, boolean isLast) {
        sb.append(prefix);
        sb.append(isLast ? "└── " : "├── ");

        // Special handling for ExecEngine to show its subPlan
        if (node instanceof ExecEngine) {
            ExecEngine engine = (ExecEngine) node;
            sb.append("ExecEngine\n");
            String childPrefix = prefix + (isLast ? "    " : "│   ");
            explainNode(engine.getSubPlan(), sb, childPrefix, true);
            return;
        }

        sb.append(node.toString());
        sb.append("\n");

        List<ExecNode> children = node.getChildren();
        for (int i = 0; i < children.size(); i++) {
            String childPrefix = prefix + (isLast ? "    " : "│   ");
            explainNode(children.get(i), sb, childPrefix, i == children.size() - 1);
        }
    }
}
