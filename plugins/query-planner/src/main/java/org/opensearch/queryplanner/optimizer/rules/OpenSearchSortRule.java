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
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.opensearch.queryplanner.physical.rel.OpenSearchConvention;
import org.opensearch.queryplanner.physical.rel.OpenSearchSort;

/**
 * Rule that converts a LogicalSort to an OpenSearchSort.
 *
 * <p>Sort operators order rows by specified columns. When combined
 * with LIMIT, they become top-N operations which are more efficient.
 */
public class OpenSearchSortRule extends ConverterRule {

    public static final OpenSearchSortRule INSTANCE = new OpenSearchSortRule(Config.INSTANCE
        .withConversion(
            LogicalSort.class,
            Convention.NONE,
            OpenSearchConvention.INSTANCE,
            "OpenSearchSortRule")
        .withRuleFactory(OpenSearchSortRule::new));

    protected OpenSearchSortRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        Sort sort = (Sort) rel;
        RelTraitSet traitSet = sort.getTraitSet().replace(OpenSearchConvention.INSTANCE);

        return new OpenSearchSort(
            sort.getCluster(),
            traitSet,
            convert(sort.getInput(), OpenSearchConvention.INSTANCE),
            sort.getCollation(),
            sort.offset,
            sort.fetch
        );
    }
}
