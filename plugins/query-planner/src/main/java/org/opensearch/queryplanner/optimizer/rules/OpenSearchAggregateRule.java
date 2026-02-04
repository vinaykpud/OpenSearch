/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.optimizer.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.opensearch.queryplanner.physical.rel.OpenSearchAggregate;
import org.opensearch.queryplanner.physical.rel.OpenSearchConvention;

/**
 * Rule that converts a LogicalAggregate to an OpenSearchAggregate.
 *
 * <p>Aggregations group rows and compute aggregate functions (SUM, COUNT, etc.).
 * In distributed execution, aggregations are split into PARTIAL (per-shard)
 * and FINAL (coordinator) phases.
 */
public class OpenSearchAggregateRule extends ConverterRule {

    public static final OpenSearchAggregateRule INSTANCE = new OpenSearchAggregateRule(Config.INSTANCE
        .withConversion(
            LogicalAggregate.class,
            Convention.NONE,
            OpenSearchConvention.INSTANCE,
            "OpenSearchAggregateRule")
        .withRuleFactory(OpenSearchAggregateRule::new));

    protected OpenSearchAggregateRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalAggregate aggregate = (LogicalAggregate) rel;
        RelTraitSet traitSet = aggregate.getTraitSet().replace(OpenSearchConvention.INSTANCE);

        return new OpenSearchAggregate(
            aggregate.getCluster(),
            traitSet,
            convert(aggregate.getInput(), OpenSearchConvention.INSTANCE),
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggregate.getAggCallList()
        );
    }
}
