/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.optimizer.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.opensearch.queryplanner.physical.rel.OpenSearchConvention;
import org.opensearch.queryplanner.physical.rel.OpenSearchScan;

/**
 * Rule that converts a LogicalTableScan to an OpenSearchScan.
 *
 * <p>This is the fundamental conversion rule - every query needs to read
 * data from somewhere, and this rule converts the logical scan to our
 * physical implementation that reads from OpenSearch indices.
 */
public class OpenSearchScanRule extends ConverterRule {

    public static final OpenSearchScanRule INSTANCE = new OpenSearchScanRule(Config.INSTANCE
        .withConversion(
            LogicalTableScan.class,
            Convention.NONE,
            OpenSearchConvention.INSTANCE,
            "OpenSearchScanRule")
        .withRuleFactory(OpenSearchScanRule::new));

    protected OpenSearchScanRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalTableScan scan = (LogicalTableScan) rel;
        RelTraitSet traitSet = scan.getTraitSet().replace(OpenSearchConvention.INSTANCE);

        return new OpenSearchScan(
            scan.getCluster(),
            traitSet,
            scan.getTable()
        );
    }
}
