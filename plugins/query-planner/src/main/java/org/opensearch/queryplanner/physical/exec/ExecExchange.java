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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Exchange node - marks distribution boundary between execution stages.
 *
 * <p>Exchange operators define how data flows between stages in distributed execution:
 * <ul>
 *   <li>{@link ExchangeType#GATHER} - Collect all data to coordinator</li>
 *   <li>{@link ExchangeType#HASH} - Repartition by hash of columns</li>
 *   <li>{@link ExchangeType#BROADCAST} - Send full copy to all nodes</li>
 *   <li>{@link ExchangeType#ROUND_ROBIN} - Distribute evenly across nodes</li>
 * </ul>
 *
 * <h2>Stage Boundaries:</h2>
 * <pre>
 * Stage 1 (coordinator):
 *   ExecAggregate(FINAL)
 *     └── ExecExchange(GATHER)  ← Stage boundary
 *
 * Stage 0 (shards):
 *           └── ExecAggregate(PARTIAL)
 *                 └── ExecScan
 * </pre>
 */
public class ExecExchange implements ExecNode {

    /** Distribution type: data collected to a single node (coordinator). */
    public static final String DISTRIBUTION_EXECUTION_SINGLETON = "EXECUTION_SINGLETON";
    /** Distribution type: data repartitioned by hash across nodes. */
    public static final String DISTRIBUTION_EXECUTION_HASH = "EXECUTION_HASH";
    /** Distribution type: data broadcast to all nodes. */
    public static final String DISTRIBUTION_EXECUTION_BROADCAST = "EXECUTION_BROADCAST";
    /** Distribution type: data distributed randomly across nodes. */
    public static final String DISTRIBUTION_EXECUTION_RANDOM = "EXECUTION_RANDOM";

    private final ExecNode input;
    private final ExchangeType exchangeType;
    private final List<Integer> hashKeys;  // For HASH exchange
    private final String distribution;

    /**
     * Type of data redistribution.
     */
    public enum ExchangeType {
        /**
         * Gather all partitions to coordinator (SINGLETON distribution).
         */
        GATHER,

        /**
         * Repartition data by hash of specified columns.
         */
        HASH,

        /**
         * Send full copy of data to all nodes.
         */
        BROADCAST,

        /**
         * Distribute rows evenly across nodes.
         */
        ROUND_ROBIN
    }

    public ExecExchange(ExecNode input, ExchangeType exchangeType) {
        this(input, exchangeType, Collections.emptyList());
    }

    public ExecExchange(ExecNode input, ExchangeType exchangeType, List<Integer> hashKeys) {
        this.input = input;
        this.exchangeType = exchangeType;
        this.hashKeys = hashKeys != null ? new ArrayList<>(hashKeys) : Collections.emptyList();
        this.distribution = computeDistribution(exchangeType);
    }

    public ExecExchange(StreamInput in) throws IOException {
        this.input = ExecNode.readNode(in);
        this.exchangeType = ExchangeType.values()[in.readVInt()];
        int keyCount = in.readVInt();
        if (keyCount > 0) {
            this.hashKeys = new ArrayList<>(keyCount);
            for (int i = 0; i < keyCount; i++) {
                this.hashKeys.add(in.readVInt());
            }
        } else {
            this.hashKeys = Collections.emptyList();
        }
        // Read distribution with default for backward compatibility
        this.distribution = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ExecNode.writeNode(out, input);
        out.writeVInt(exchangeType.ordinal());
        out.writeVInt(hashKeys.size());
        for (Integer key : hashKeys) {
            out.writeVInt(key);
        }
        out.writeOptionalString(distribution);
    }

    /**
     * Compute the distribution type from the exchange type.
     */
    private static String computeDistribution(ExchangeType type) {
        switch (type) {
            case GATHER:
                return DISTRIBUTION_EXECUTION_SINGLETON;
            case HASH:
                return DISTRIBUTION_EXECUTION_HASH;
            case BROADCAST:
                return DISTRIBUTION_EXECUTION_BROADCAST;
            case ROUND_ROBIN:
                return DISTRIBUTION_EXECUTION_RANDOM;
            default:
                return "UNKNOWN";
        }
    }

    @Override
    public NodeType getType() {
        return NodeType.EXCHANGE;
    }

    @Override
    public List<ExecNode> getChildren() {
        return Collections.singletonList(input);
    }

    public ExecNode getInput() {
        return input;
    }

    public ExchangeType getExchangeType() {
        return exchangeType;
    }

    /**
     * Get the hash keys for HASH exchange type.
     * @return List of column indices to hash by, empty for non-HASH exchanges
     */
    public List<Integer> getHashKeys() {
        return Collections.unmodifiableList(hashKeys);
    }

    /**
     * Get the target distribution of this exchange.
     */
    public String getDistribution() {
        return distribution != null ? distribution : computeDistribution(exchangeType);
    }

    /**
     * Check if this is a gathering exchange (collects to coordinator).
     */
    public boolean isGather() {
        return exchangeType == ExchangeType.GATHER;
    }

    /**
     * Check if this exchange involves network shuffling.
     */
    public boolean requiresNetworkShuffle() {
        return exchangeType == ExchangeType.HASH || exchangeType == ExchangeType.BROADCAST;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ExecExchange[").append(exchangeType);
        if (exchangeType == ExchangeType.HASH && !hashKeys.isEmpty()) {
            sb.append(", keys=").append(hashKeys);
        }
        sb.append(", dist=").append(getDistribution());
        sb.append("]");
        return sb.toString();
    }
}
