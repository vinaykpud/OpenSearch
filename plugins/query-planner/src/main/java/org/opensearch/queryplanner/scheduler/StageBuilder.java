/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.scheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.queryplanner.physical.exec.ExecExchange;
import org.opensearch.queryplanner.physical.exec.ExecNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits an ExecNode tree at Exchange boundaries into stages.
 *
 * <p>For scatter-gather (GATHER exchange), creates two stages:
 * <ul>
 *   <li>LEAF stage: runs on each shard (below the exchange)</li>
 *   <li>ROOT stage: runs on coordinator (above the exchange)</li>
 * </ul>
 *
 * <pre>
 * Input plan:
 *   ExecAggregate(FINAL)
 *     └── ExecExchange(GATHER)
 *           └── ExecAggregate(PARTIAL)
 *                 └── ExecScan
 *
 * After staging:
 *   LEAF stage (id=0):
 *     ExecAggregate(PARTIAL)
 *       └── ExecScan
 *
 *   ROOT stage (id=1):
 *     ExecAggregate(FINAL)
 *       └── [fed from LEAF results]
 * </pre>
 */
class StageBuilder {

    private static final Logger logger = LogManager.getLogger(StageBuilder.class);

    /**
     * Result of plan staging.
     */
    static class StagedPlan {
        private final QueryStage leafStage;
        private final QueryStage rootStage;
        private final boolean hasExchange;

        StagedPlan(QueryStage leafStage, QueryStage rootStage, boolean hasExchange) {
            this.leafStage = leafStage;
            this.rootStage = rootStage;
            this.hasExchange = hasExchange;
        }

        /** Stage to execute on shards (null if no exchange). */
        public QueryStage getLeafStage() {
            return leafStage;
        }

        /** Stage to execute on coordinator after gathering (null if exchange at root). */
        public QueryStage getRootStage() {
            return rootStage;
        }

        /** Whether the plan has an exchange (requires distributed execution). */
        public boolean hasExchange() {
            return hasExchange;
        }
    }

    /**
     * Build stages from the plan by splitting at Exchange boundaries.
     *
     * @param root The root of the ExecNode tree
     * @return StagedPlan with LEAF and ROOT stages
     */
    public StagedPlan build(ExecNode root) {
        // Find the first GATHER exchange
        ExchangeLocation location = findGatherExchange(root, null, -1);

        if (location == null) {
            // No exchange - single stage execution (no distribution)
            logger.debug("No exchange found, single-stage execution");
            return new StagedPlan(
                new QueryStage(0, QueryStage.StageType.LEAF, root),
                null,
                false
            );
        }

        ExecExchange exchange = location.exchange;
        ExecNode leafFragment = exchange.getInput();

        logger.debug("Found GATHER exchange, splitting plan");
        logger.debug("  LEAF fragment: {}", leafFragment);

        // LEAF stage: the subtree below the exchange
        QueryStage leafStage = new QueryStage(0, QueryStage.StageType.LEAF, leafFragment);

        // ROOT stage: the nodes above the exchange
        // If exchange is at root, there's no ROOT stage (just gather and return)
        QueryStage rootStage = null;
        if (location.parent != null) {
            // Replace the exchange in parent with a placeholder
            ExecNode rootFragment = replaceExchangeWithInput(root, location);
            rootStage = new QueryStage(1, QueryStage.StageType.ROOT, rootFragment);
            logger.debug("  ROOT fragment: {}", rootFragment);
        }

        return new StagedPlan(leafStage, rootStage, true);
    }

    /**
     * Location of an exchange in the tree.
     */
    private static class ExchangeLocation {
        final ExecExchange exchange;
        final ExecNode parent;
        final int childIndex;

        ExchangeLocation(ExecExchange exchange, ExecNode parent, int childIndex) {
            this.exchange = exchange;
            this.parent = parent;
            this.childIndex = childIndex;
        }
    }

    /**
     * Find the first GATHER exchange in the tree.
     */
    private ExchangeLocation findGatherExchange(ExecNode node, ExecNode parent, int childIndex) {
        if (node instanceof ExecExchange) {
            ExecExchange exchange = (ExecExchange) node;
            if (exchange.getExchangeType() == ExecExchange.ExchangeType.GATHER) {
                return new ExchangeLocation(exchange, parent, childIndex);
            }
        }

        // Search children
        List<ExecNode> children = node.getChildren();
        for (int i = 0; i < children.size(); i++) {
            ExchangeLocation found = findGatherExchange(children.get(i), node, i);
            if (found != null) {
                return found;
            }
        }

        return null;
    }

    /**
     * Create a copy of the tree above the exchange, with the exchange removed.
     * The exchange's position becomes the input point for gathered data.
     */
    private ExecNode replaceExchangeWithInput(ExecNode root, ExchangeLocation location) {
        // For now, return the root with exchange's child removed
        // The coordinator will feed gathered data directly into the root operators
        return removeExchangeSubtree(root, location.exchange);
    }

    /**
     * Remove the exchange and its subtree, returning a modified tree.
     * Returns null if the node itself is the exchange.
     */
    private ExecNode removeExchangeSubtree(ExecNode node, ExecExchange exchangeToRemove) {
        if (node == exchangeToRemove) {
            return null;
        }

        List<ExecNode> children = node.getChildren();
        if (children.isEmpty()) {
            return node;
        }

        // Check if any child is the exchange
        List<ExecNode> newChildren = new ArrayList<>();
        boolean modified = false;
        for (ExecNode child : children) {
            if (child == exchangeToRemove) {
                // Skip this child (exchange removed)
                modified = true;
            } else {
                ExecNode newChild = removeExchangeSubtree(child, exchangeToRemove);
                if (newChild != child) {
                    modified = true;
                }
                if (newChild != null) {
                    newChildren.add(newChild);
                }
            }
        }

        if (!modified) {
            return node;
        }

        // Need to create a new node with updated children
        // For now, return the original node - the coordinator will handle this
        // by knowing the ROOT stage consumes gathered data
        return node;
    }
}
