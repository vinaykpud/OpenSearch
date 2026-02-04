/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.optimizer;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.opensearch.queryplanner.physical.rel.OpenSearchExchange;

/**
 * Calcite trait definition for data distribution across OpenSearch shards.
 *
 * <p>This trait def tells the VolcanoPlanner:
 * <ul>
 *   <li>What distribution traits exist (SINGLETON, RANDOM, HASH, etc.)</li>
 *   <li>How to convert between distributions (via Exchange operators)</li>
 *   <li>Whether a distribution satisfies a requirement</li>
 * </ul>
 *
 * <h2>How it works:</h2>
 * <pre>
 * 1. Each RelNode has a distribution trait (e.g., TableScan produces RANDOM)
 * 2. Some operators require specific input distributions (e.g., final agg needs SINGLETON)
 * 3. When requirement doesn't match, planner calls convert() to insert enforcer
 * 4. convert() creates an Exchange operator to transform the distribution
 * </pre>
 *
 * <h2>Example:</h2>
 * <pre>
 * OpenSearchAggregate(FINAL) requires SINGLETON
 *   └── OpenSearchScan produces RANDOM
 *
 * Planner sees mismatch, calls convert(RANDOM → SINGLETON)
 * Result:
 *   OpenSearchAggregate(FINAL) [SINGLETON]
 *     └── OpenSearchExchange(GATHER) [converts RANDOM → SINGLETON]
 *           └── OpenSearchScan [RANDOM]
 * </pre>
 */
public class OpenSearchDistributionTraitDef extends RelTraitDef<RelDistribution> {

    /**
     * Singleton instance of this trait def.
     */
    public static final OpenSearchDistributionTraitDef INSTANCE = new OpenSearchDistributionTraitDef();

    private OpenSearchDistributionTraitDef() {
        // Singleton
    }

    @Override
    public Class<RelDistribution> getTraitClass() {
        return RelDistribution.class;
    }

    @Override
    public String getSimpleName() {
        return "dist";
    }

    @Override
    public RelDistribution getDefault() {
        return OpenSearchDistribution.ANY;
    }

    /**
     * Convert a RelNode to have the required distribution trait.
     *
     * <p>This is called by VolcanoPlanner when it needs a node with a specific
     * distribution but the current node has a different distribution.
     *
     * @param planner The planner
     * @param rel The RelNode to convert
     * @param toDistribution The required distribution
     * @return A new RelNode with the required distribution, or null if conversion not possible
     */
    @Override
    public RelNode convert(RelOptPlanner planner, RelNode rel,
                           RelDistribution toDistribution, boolean allowInfiniteCostConverters) {

        if (!(toDistribution instanceof OpenSearchDistribution)) {
            return null;
        }

        OpenSearchDistribution required = (OpenSearchDistribution) toDistribution;
        RelDistribution currentTrait = rel.getTraitSet().getTrait(this);

        if (currentTrait == null) {
            currentTrait = OpenSearchDistribution.ANY;
        }

        // If already satisfies, no conversion needed
        if (currentTrait.satisfies(required)) {
            return rel;
        }

        // Determine exchange type based on required distribution
        OpenSearchExchange.ExchangeType exchangeType = determineExchangeType(required);
        if (exchangeType == null) {
            return null;  // Cannot convert
        }

        // Create new trait set with required distribution
        RelTraitSet newTraits = rel.getTraitSet().replace(required);

        // Create Exchange to enforce the distribution
        return OpenSearchExchange.create(
            rel.getCluster(),
            newTraits,
            rel,
            exchangeType,
            required.getKeys()
        );
    }

    /**
     * Determine which exchange type to use for the required distribution.
     */
    private OpenSearchExchange.ExchangeType determineExchangeType(OpenSearchDistribution required) {
        switch (required.getType()) {
            case SINGLETON:
                return OpenSearchExchange.ExchangeType.GATHER;
            case HASH_DISTRIBUTED:
                return OpenSearchExchange.ExchangeType.HASH;
            case BROADCAST_DISTRIBUTED:
                return OpenSearchExchange.ExchangeType.BROADCAST;
            case RANDOM_DISTRIBUTED:
                return OpenSearchExchange.ExchangeType.ROUND_ROBIN;
            case ANY:
                return null;  // No conversion needed
            default:
                return null;
        }
    }

    @Override
    public boolean canConvert(RelOptPlanner planner, RelDistribution fromTrait, RelDistribution toTrait) {
        if (!(fromTrait instanceof OpenSearchDistribution) || !(toTrait instanceof OpenSearchDistribution)) {
            return false;
        }

        // Can always convert via Exchange, except:
        // - ANY doesn't need conversion
        // - Same distribution doesn't need conversion
        if (toTrait.getType() == RelDistribution.Type.ANY) {
            return true;
        }

        return !fromTrait.equals(toTrait);
    }

    @Override
    public void registerConverterRule(RelOptPlanner planner, ConverterRule converterRule) {
        // We handle conversion via convert() method, not converter rules
    }
}
