/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.physical.exec;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.queryplanner.physical.rel.OpenSearchRel;

/**
 * Converts a physical Calcite RelNode tree to an ExecNode tree.
 *
 * <p>This is the bridge between Calcite's planning world and our
 * execution world. After this conversion:
 * <ul>
 *   <li>No more Calcite dependencies</li>
 *   <li>ExecNode tree is Writeable (can be serialized)</li>
 *   <li>Ready for execution via OperatorFactory</li>
 * </ul>
 *
 * <h2>Conversion Flow:</h2>
 * <pre>
 * OpenSearchProject (RelNode)
 *   └── OpenSearchFilter (RelNode)
 *         └── OpenSearchScan (RelNode)
 *
 *                    │ convert()
 *                    ▼
 *
 * ExecProject (ExecNode)
 *   └── ExecFilter (ExecNode)
 *         └── ExecScan (ExecNode)
 * </pre>
 */
public class RelToExecConverter {

    private static final Logger logger = LogManager.getLogger(RelToExecConverter.class);

    /**
     * Convert a physical RelNode tree to an ExecNode tree.
     *
     * @param physicalPlan The root of the physical plan (must be OpenSearchRel)
     * @return The root of the ExecNode tree
     * @throws IllegalArgumentException if the plan contains non-OpenSearchRel nodes
     */
    public ExecNode convert(RelNode physicalPlan) {
        if (!(physicalPlan instanceof OpenSearchRel)) {
            throw new IllegalArgumentException(
                "Expected OpenSearchRel but got: " + physicalPlan.getClass().getName() +
                ". Did you forget to run physical optimization?");
        }

        OpenSearchRel osRel = (OpenSearchRel) physicalPlan;
        ExecNode execNode = osRel.toExecNode();

        logger.debug("Converted RelNode to ExecNode: {} -> {}",
            physicalPlan.getClass().getSimpleName(),
            execNode.getClass().getSimpleName());

        return execNode;
    }

    /**
     * Convert and validate the entire plan tree.
     *
     * <p>This method walks the tree and ensures all nodes are properly
     * converted. It also logs the plan structure for debugging.
     *
     * @param physicalPlan The root of the physical plan
     * @return The root of the ExecNode tree
     */
    public ExecNode convertWithValidation(RelNode physicalPlan) {
        validatePlanTree(physicalPlan, 0);
        ExecNode result = convert(physicalPlan);
        logExecTree(result, 0);
        return result;
    }

    /**
     * Validate that all nodes in the tree are OpenSearchRel.
     */
    private void validatePlanTree(RelNode node, int depth) {
        String indent = "  ".repeat(depth);

        if (!(node instanceof OpenSearchRel)) {
            throw new IllegalStateException(
                indent + "Non-OpenSearchRel node found: " + node.getClass().getName() +
                " at depth " + depth + ". Plan may not be fully optimized.");
        }

        logger.debug("{}RelNode: {} ({})", indent,
            node.getClass().getSimpleName(),
            node.getRowType().getFieldNames());

        for (RelNode input : node.getInputs()) {
            validatePlanTree(input, depth + 1);
        }
    }

    /**
     * Log the ExecNode tree structure for debugging.
     */
    private void logExecTree(ExecNode node, int depth) {
        String indent = "  ".repeat(depth);
        logger.debug("{}ExecNode: {}", indent, node);

        for (ExecNode child : node.getChildren()) {
            logExecTree(child, depth + 1);
        }
    }
}
