/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.optimizer.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.queryplanner.physical.rel.OpenSearchExchange;
import org.opensearch.queryplanner.physical.rel.OpenSearchNativeEngine;
import org.opensearch.queryplanner.physical.rel.OpenSearchNativeScan;
import org.opensearch.queryplanner.physical.rel.OpenSearchRel;
import org.opensearch.queryplanner.physical.rel.OpenSearchScan;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule that wraps subtrees below Exchange in OpenSearchNativeEngine for native execution.
 *
 * <p>This rule fires at Exchange boundaries and wraps the appropriate subtree
 * in a OpenSearchNativeEngine node. The wrapped subtree will be executed by a native
 * engine (e.g., DataFusion) or Java fallback.
 *
 * <h2>Scan Mode (controlled by nativeScanEnabled flag):</h2>
 * <ul>
 *   <li><b>nativeScanEnabled=false</b> (default): Scan is a boundary node.
 *       Lucene provides data via Arrow IPC to the engine.</li>
 *   <li><b>nativeScanEnabled=true</b>: Scan is included in engine subtree.
 *       The native engine reads data directly from columnar storage.</li>
 * </ul>
 *
 * <p>Note: This rule should run in a HepPlanner (deterministic) AFTER the
 * VolcanoPlanner (cost-based) has produced the final physical plan.
 */
public class OpenSearchNativeEngineRule extends RelOptRule {

    private static final Logger logger = LogManager.getLogger(OpenSearchNativeEngineRule.class);

    /**
     * When true, scans are included in the engine subtree (native engine reads directly).
     * When false, scans are boundary nodes (Lucene provides data via Arrow IPC).
     */
    private final boolean nativeScanEnabled;

    /**
     * Create a rule with specified native scan mode.
     *
     * @param nativeScanEnabled If true, native engine reads data directly.
     *                          If false, Lucene doc values. // TODO: this should be based on index setting
     */
    public OpenSearchNativeEngineRule(boolean nativeScanEnabled) {
        super(operand(OpenSearchExchange.class, any()), "OpenSearchNativeEngineRule");
        this.nativeScanEnabled = nativeScanEnabled;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchExchange exchange = call.rel(0);
        RelNode input = unwrap(exchange.getInput());

        logger.debug("OpenSearchNativeEngineRule.matches: input={}", input.getClass().getSimpleName());

        // Don't wrap if input is already OpenSearchNativeEngine
        if (input instanceof OpenSearchNativeEngine) {
            logger.debug("Already wrapped, skipping");
            return false;
        }

        // Only wrap OpenSearchRel nodes
        if (!(input instanceof OpenSearchRel)) {
            logger.debug("Not OpenSearchRel: {}, skipping", input.getClass().getSimpleName());
            return false;
        }

        // Wrap all OpenSearchRel nodes including scans
        // - OpenSearchScan (Lucene): engine receives Arrow data from scan, applies no-ops
        // - OpenSearchNativeScan: engine reads columnar data directly
        // - Other nodes (filter, project, etc.): engine executes these operations
        logger.info("Rule matches! Will wrap {}", input.getClass().getSimpleName());
        return true;
    }

    /**
     * Unwrap HepRelVertex to get the underlying RelNode.
     * HepPlanner wraps all nodes in HepRelVertex for tracking.
     */
    private RelNode unwrap(RelNode node) {
        if (node instanceof HepRelVertex) {
            return ((HepRelVertex) node).getCurrentRel();
        }
        return node;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchExchange exchange = call.rel(0);
        RelNode rawInput = exchange.getInput();
        RelNode input = unwrap(rawInput);

        logger.info("Wrapping subtree in OpenSearchNativeEngine (nativeScan={}): {}",
            nativeScanEnabled, input.getRelTypeName());

        // Find the scan node at the bottom (required as input to satisfy SingleRel)
        RelNode scanNode = findScan(input);

        // Create OpenSearchNativeEngine wrapping the subtree
        // Pass nativeScanEnabled flag so it knows how to convert scans to ExecNode
        OpenSearchNativeEngine engineNode = new OpenSearchNativeEngine(
            exchange.getCluster(),
            input.getTraitSet(),
            scanNode,  // The scan node (satisfies SingleRel input requirement)
            input,     // The entire subtree to execute in engine
            nativeScanEnabled  // Flag for scan conversion
        );

        // Create new exchange with OpenSearchNativeEngine as input
        RelNode newExchange = exchange.copy(exchange.getTraitSet(), java.util.List.of(engineNode));

        logger.info("Transformed to: {}", newExchange.getClass().getSimpleName());
        call.transformTo(newExchange);
    }

    /**
     * Find the scan node at the bottom of a subtree.
     *
     * <p>The scan is always returned as the "input" to satisfy Calcite's SingleRel requirement.
     * The nativeScanEnabled flag only controls how scans are converted to ExecNode.
     *
     * @return The scan node at the bottom of the tree
     */
    private RelNode findScan(RelNode node) {
        // Unwrap HepRelVertex if present
        node = unwrap(node);

        // Check if this is a scan node
        if (node instanceof OpenSearchScan || node instanceof OpenSearchNativeScan) {
            return node;
        }

        // Leaf node that's not a scan
        if (node.getInputs().isEmpty()) {
            throw new IllegalStateException("No scan found in subtree");
        }

        // Follow the first input (assumes linear plan for now)
        return findScan(node.getInput(0));
    }
}
