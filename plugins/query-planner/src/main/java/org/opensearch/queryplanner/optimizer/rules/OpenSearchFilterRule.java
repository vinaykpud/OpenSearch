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
import org.apache.calcite.rel.logical.LogicalFilter;
import org.opensearch.queryplanner.physical.rel.OpenSearchConvention;
import org.opensearch.queryplanner.physical.rel.OpenSearchFilter;

/**
 * Rule that converts a LogicalFilter to an OpenSearchFilter.
 *
 * <p>Filters select rows based on a predicate. After conversion,
 * further optimization rules may push the filter into the scan
 * for Lucene-level execution.
 */
public class OpenSearchFilterRule extends ConverterRule {

    public static final OpenSearchFilterRule INSTANCE = new OpenSearchFilterRule(Config.INSTANCE
        .withConversion(
            LogicalFilter.class,
            Convention.NONE,
            OpenSearchConvention.INSTANCE,
            "OpenSearchFilterRule")
        .withRuleFactory(OpenSearchFilterRule::new));

    protected OpenSearchFilterRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalFilter filter = (LogicalFilter) rel;
        RelTraitSet traitSet = filter.getTraitSet().replace(OpenSearchConvention.INSTANCE);

        return new OpenSearchFilter(
            filter.getCluster(),
            traitSet,
            convert(filter.getInput(), OpenSearchConvention.INSTANCE),
            filter.getCondition()
        );
    }
}
