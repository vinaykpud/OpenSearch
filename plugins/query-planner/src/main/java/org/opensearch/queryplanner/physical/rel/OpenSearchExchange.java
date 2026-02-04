/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.physical.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableIntList;
import org.opensearch.queryplanner.physical.exec.ExecExchange;
import org.opensearch.queryplanner.physical.exec.ExecNode;

import java.util.List;

/**
 * Exchange operator - redistributes data across nodes.
 *
 * <p>This operator is inserted by the planner when the distribution trait
 * of a child doesn't match the requirement of its parent. It defines
 * the boundary between execution stages.
 *
 * <h2>Exchange Types:</h2>
 * <ul>
 *   <li>{@link ExchangeType#GATHER} - Collect all data to coordinator (SINGLETON)</li>
 *   <li>{@link ExchangeType#HASH} - Repartition by hash of columns</li>
 *   <li>{@link ExchangeType#BROADCAST} - Send full copy to all nodes</li>
 *   <li>{@link ExchangeType#ROUND_ROBIN} - Distribute evenly across nodes</li>
 * </ul>
 *
 * <h2>Stage Boundaries:</h2>
 * <p>Exchanges define where the plan is split into stages for distributed execution:
 * <pre>
 * Stage 1 (coordinator):  Aggregate(FINAL)
 *                              │
 *                         Exchange(GATHER)  ← Stage boundary
 *                              │
 * Stage 0 (shards):       Aggregate(PARTIAL)
 *                              │
 *                           Scan
 * </pre>
 */
public class OpenSearchExchange extends SingleRel implements OpenSearchRel {

    /**
     * Type of data redistribution.
     */
    public enum ExchangeType {
        /**
         * Gather all partitions to a single node (coordinator).
         * Used for final aggregation, sorting with limit, etc.
         */
        GATHER,

        /**
         * Repartition data by hash of specified columns.
         * Used for distributed GROUP BY, hash joins, etc.
         */
        HASH,

        /**
         * Send full copy of data to all nodes.
         * Used for broadcast joins with small tables.
         */
        BROADCAST,

        /**
         * Distribute rows evenly across nodes.
         * Used for load balancing.
         */
        ROUND_ROBIN
    }

    private final ExchangeType exchangeType;
    private final ImmutableIntList hashKeys;  // For HASH type

    protected OpenSearchExchange(RelOptCluster cluster, RelTraitSet traits,
                                  RelNode input, ExchangeType exchangeType,
                                  ImmutableIntList hashKeys) {
        super(cluster, traits, input);
        this.exchangeType = exchangeType;
        this.hashKeys = hashKeys != null ? hashKeys : ImmutableIntList.of();
    }

    /**
     * Create an Exchange operator.
     *
     * @param cluster The cluster
     * @param traits The trait set (should include target distribution)
     * @param input The input RelNode
     * @param exchangeType The type of exchange
     * @param hashKeys For HASH exchange, the columns to hash by
     * @return New OpenSearchExchange
     */
    public static OpenSearchExchange create(RelOptCluster cluster, RelTraitSet traits,
                                             RelNode input, ExchangeType exchangeType,
                                             ImmutableIntList hashKeys) {
        return new OpenSearchExchange(cluster, traits, input, exchangeType, hashKeys);
    }

    /**
     * Create a GATHER exchange (collect to coordinator).
     */
    public static OpenSearchExchange gather(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        return new OpenSearchExchange(cluster, traits, input, ExchangeType.GATHER, ImmutableIntList.of());
    }

    /**
     * Create a HASH exchange (repartition by columns).
     */
    public static OpenSearchExchange hash(RelOptCluster cluster, RelTraitSet traits,
                                           RelNode input, List<Integer> keys) {
        return new OpenSearchExchange(cluster, traits, input, ExchangeType.HASH, ImmutableIntList.copyOf(keys));
    }

    /**
     * Create a BROADCAST exchange (send to all nodes).
     */
    public static OpenSearchExchange broadcast(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        return new OpenSearchExchange(cluster, traits, input, ExchangeType.BROADCAST, ImmutableIntList.of());
    }

    public ExchangeType getExchangeType() {
        return exchangeType;
    }

    public ImmutableIntList getHashKeys() {
        return hashKeys;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = mq.getRowCount(this);
        double cpu = rows;  // Processing cost

        // Network cost varies by exchange type
        double network;
        switch (exchangeType) {
            case GATHER:
                // All data flows to coordinator
                network = rows;
                break;
            case HASH:
                // Data shuffled across nodes
                network = rows;
                break;
            case BROADCAST:
                // Data replicated to all nodes (expensive)
                network = rows * 10;  // Multiplier for broadcast cost
                break;
            case ROUND_ROBIN:
                network = rows;
                break;
            default:
                network = rows;
        }

        return planner.getCostFactory().makeCost(rows, cpu, network);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchExchange(getCluster(), traitSet, sole(inputs), exchangeType, hashKeys);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("exchangeType", exchangeType)
            .itemIf("hashKeys", hashKeys, exchangeType == ExchangeType.HASH && !hashKeys.isEmpty());
    }

    @Override
    public ExecNode toExecNode() {
        // Convert child first
        OpenSearchRel childRel = (OpenSearchRel) getInput();
        ExecNode childExec = childRel.toExecNode();

        // Map exchange type
        ExecExchange.ExchangeType execType;
        switch (exchangeType) {
            case GATHER:
                execType = ExecExchange.ExchangeType.GATHER;
                break;
            case HASH:
                execType = ExecExchange.ExchangeType.HASH;
                break;
            case BROADCAST:
                execType = ExecExchange.ExchangeType.BROADCAST;
                break;
            case ROUND_ROBIN:
                execType = ExecExchange.ExchangeType.ROUND_ROBIN;
                break;
            default:
                throw new IllegalStateException("Unknown exchange type: " + exchangeType);
        }

        // Convert hash keys to list
        List<Integer> keys = hashKeys.toIntegerList();

        return new ExecExchange(childExec, execType, keys);
    }
}
