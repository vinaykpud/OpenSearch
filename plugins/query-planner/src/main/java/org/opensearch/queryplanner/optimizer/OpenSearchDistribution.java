/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.optimizer;

import org.apache.calcite.plan.RelMultipleTrait;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings.TargetMapping;

import java.util.List;
import java.util.Objects;

/**
 * Represents data distribution across OpenSearch shards.
 *
 * <p>This trait is used by the Calcite planner to track how data is distributed
 * and to automatically insert Exchange operators when distribution requirements
 * don't match.
 *
 * <h2>Distribution Types:</h2>
 * <ul>
 *   <li>{@link Type#SINGLETON} - All data on coordinator (after GATHER)</li>
 *   <li>{@link Type#RANDOM_DISTRIBUTED} - Data spread across shards (table scan output)</li>
 *   <li>{@link Type#HASH_DISTRIBUTED} - Partitioned by hash of columns</li>
 *   <li>{@link Type#BROADCAST_DISTRIBUTED} - Full copy on all nodes</li>
 *   <li>{@link Type#ANY} - No specific distribution requirement</li>
 * </ul>
 */
public class OpenSearchDistribution implements RelDistribution {

    /**
     * Singleton instance - all data on one node (coordinator).
     */
    public static final OpenSearchDistribution SINGLETON =
        new OpenSearchDistribution(Type.SINGLETON, ImmutableIntList.of());

    /**
     * Random distribution - data spread across shards without specific partitioning.
     * This is what TableScan produces.
     */
    public static final OpenSearchDistribution RANDOM =
        new OpenSearchDistribution(Type.RANDOM_DISTRIBUTED, ImmutableIntList.of());

    /**
     * Any distribution - no specific requirement, accepts any input distribution.
     */
    public static final OpenSearchDistribution ANY =
        new OpenSearchDistribution(Type.ANY, ImmutableIntList.of());

    /**
     * Broadcast distribution - full copy of data on all nodes.
     */
    public static final OpenSearchDistribution BROADCAST =
        new OpenSearchDistribution(Type.BROADCAST_DISTRIBUTED, ImmutableIntList.of());

    private final Type type;
    private final ImmutableIntList keys;

    private OpenSearchDistribution(Type type, ImmutableIntList keys) {
        this.type = type;
        this.keys = keys;
    }

    /**
     * Create a hash distribution partitioned by the given column indices.
     *
     * @param keys Column indices to hash by
     * @return Hash distribution
     */
    public static OpenSearchDistribution hash(List<Integer> keys) {
        return new OpenSearchDistribution(Type.HASH_DISTRIBUTED, ImmutableIntList.copyOf(keys));
    }

    /**
     * Create a hash distribution partitioned by the given column indices.
     *
     * @param keys Column indices to hash by
     * @return Hash distribution
     */
    public static OpenSearchDistribution hash(int... keys) {
        return new OpenSearchDistribution(Type.HASH_DISTRIBUTED, ImmutableIntList.of(keys));
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public ImmutableIntList getKeys() {
        return keys;
    }

    @Override
    public RelTraitDef<RelDistribution> getTraitDef() {
        return OpenSearchDistributionTraitDef.INSTANCE;
    }

    @Override
    public boolean satisfies(RelTrait trait) {
        if (!(trait instanceof OpenSearchDistribution)) {
            return false;
        }
        OpenSearchDistribution required = (OpenSearchDistribution) trait;

        // ANY is satisfied by anything
        if (required.type == Type.ANY) {
            return true;
        }

        // Same type and keys satisfies
        if (this.type == required.type) {
            if (this.type == Type.HASH_DISTRIBUTED) {
                // For hash, keys must match
                return this.keys.equals(required.keys);
            }
            return true;
        }

        // SINGLETON satisfies SINGLETON only
        // RANDOM does not satisfy SINGLETON (needs GATHER)
        // HASH does not satisfy SINGLETON (needs GATHER)
        return false;
    }

    @Override
    public void register(RelOptPlanner planner) {
        // Nothing to register
    }

    @Override
    public RelDistribution apply(TargetMapping mapping) {
        // Apply column mapping to hash keys
        if (type == Type.HASH_DISTRIBUTED && !keys.isEmpty()) {
            int[] mappedKeys = new int[keys.size()];
            for (int i = 0; i < keys.size(); i++) {
                mappedKeys[i] = mapping.getTargetOpt(keys.get(i));
            }
            return new OpenSearchDistribution(type, ImmutableIntList.of(mappedKeys));
        }
        return this;
    }

    @Override
    public boolean isTop() {
        // ANY is the "top" (most general) distribution
        return type == Type.ANY;
    }

    @Override
    public int compareTo(RelMultipleTrait other) {
        if (!(other instanceof OpenSearchDistribution)) {
            return this.getClass().getName().compareTo(other.getClass().getName());
        }
        OpenSearchDistribution that = (OpenSearchDistribution) other;
        int cmp = this.type.compareTo(that.type);
        if (cmp != 0) {
            return cmp;
        }
        return this.keys.toString().compareTo(that.keys.toString());
    }

    /**
     * Check if this distribution is partitioned (not singleton).
     */
    public boolean isPartitioned() {
        return type == Type.RANDOM_DISTRIBUTED || type == Type.HASH_DISTRIBUTED;
    }

    /**
     * Check if this is a singleton distribution.
     */
    public boolean isSingleton() {
        return type == Type.SINGLETON;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenSearchDistribution that = (OpenSearchDistribution) o;
        return type == that.type && Objects.equals(keys, that.keys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, keys);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(type.name());
        if (type == Type.HASH_DISTRIBUTED && !keys.isEmpty()) {
            sb.append("(").append(keys).append(")");
        }
        return sb.toString();
    }
}
