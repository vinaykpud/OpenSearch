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
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.queryplanner.physical.exec.ExecAggregate;
import org.opensearch.queryplanner.physical.rel.OpenSearchAggregate;

/**
 * Rule to split aggregates into PARTIAL and FINAL modes for distributed execution.
 *
 * <h2>Why Split?</h2>
 * <p>In distributed execution, aggregations must be computed in two phases:
 * <ol>
 *   <li>PARTIAL: Each shard computes local aggregates for its data</li>
 *   <li>FINAL: Coordinator merges partial results into final aggregates</li>
 * </ol>
 *
 * <h2>Example:</h2>
 * <pre>
 * Input (FULL aggregate):
 *   OpenSearchAggregate(mode=FULL, group=[category], agg=[SUM(price)])
 *     └── OpenSearchScan(orders)
 *
 * Output (PARTIAL + FINAL):
 *   OpenSearchAggregate(mode=FINAL, group=[category], agg=[SUM])
 *     └── OpenSearchAggregate(mode=PARTIAL, group=[category], agg=[SUM])
 *           └── OpenSearchScan(orders)
 * </pre>
 *
 * <h2>Distribution Implications:</h2>
 * <ul>
 *   <li>PARTIAL preserves input distribution (runs on shards with RANDOM dist)</li>
 *   <li>FINAL requires SINGLETON distribution (triggers GATHER exchange)</li>
 * </ul>
 *
 * <p>The distribution trait enforcement will automatically insert an Exchange
 * between PARTIAL and FINAL because:
 * <ul>
 *   <li>PARTIAL produces RANDOM distribution</li>
 *   <li>FINAL requires SINGLETON distribution</li>
 *   <li>Planner inserts Exchange(GATHER) to convert RANDOM → SINGLETON</li>
 * </ul>
 */
public class AggregatePartialRule extends RelOptRule {

    private static final Logger logger = LogManager.getLogger(AggregatePartialRule.class);

    /**
     * Singleton instance of this rule.
     */
    public static final AggregatePartialRule INSTANCE = new AggregatePartialRule();

    private AggregatePartialRule() {
        super(operand(OpenSearchAggregate.class, any()), "AggregatePartialRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchAggregate agg = call.rel(0);
        logger.debug("AggregatePartialRule.onMatch called for aggregate mode={}, traits={}",
            agg.getMode(), agg.getTraitSet());

        // Only split FULL aggregates (not already split)
        if (agg.getMode() != ExecAggregate.AggMode.FULL) {
            logger.debug("Skipping - not a FULL aggregate");
            return;
        }

        logger.debug("Splitting FULL aggregate into PARTIAL + Exchange + FINAL");

        // Create PARTIAL aggregate (runs on shards, preserves RANDOM distribution)
        OpenSearchAggregate partial = agg.asPartial();

        // Insert Exchange(GATHER) between PARTIAL and FINAL
        // This converts RANDOM -> SINGLETON
        org.apache.calcite.plan.RelTraitSet singletonTraits = partial.getTraitSet()
            .replace(org.opensearch.queryplanner.optimizer.OpenSearchDistribution.SINGLETON);

        org.opensearch.queryplanner.physical.rel.OpenSearchExchange exchange =
            org.opensearch.queryplanner.physical.rel.OpenSearchExchange.gather(
                agg.getCluster(),
                singletonTraits,
                partial
            );

        // Create FINAL aggregate (runs on coordinator, takes gathered input)
        OpenSearchAggregate finalAgg = new OpenSearchAggregate(
            agg.getCluster(),
            singletonTraits,  // FINAL also has SINGLETON
            exchange,         // Input is the exchange
            agg.getGroupSet(),
            agg.getGroupSets(),
            agg.getAggCallList(),
            ExecAggregate.AggMode.FINAL
        );

        logger.debug("Created split plan: FINAL({}) -> Exchange -> PARTIAL({})",
            finalAgg.getTraitSet(), partial.getTraitSet());
        call.transformTo(finalAgg);
    }
}
