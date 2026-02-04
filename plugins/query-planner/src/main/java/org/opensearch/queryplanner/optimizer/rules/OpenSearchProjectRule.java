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
import org.apache.calcite.rel.logical.LogicalProject;
import org.opensearch.queryplanner.physical.rel.OpenSearchConvention;
import org.opensearch.queryplanner.physical.rel.OpenSearchProject;

/**
 * Rule that converts a LogicalProject to an OpenSearchProject.
 *
 * <p>Projects select and transform columns. Simple projections
 * (just column selection) may be pushed into the scan for
 * column pruning optimization.
 */
public class OpenSearchProjectRule extends ConverterRule {

    public static final OpenSearchProjectRule INSTANCE = new OpenSearchProjectRule(Config.INSTANCE
        .withConversion(
            LogicalProject.class,
            Convention.NONE,
            OpenSearchConvention.INSTANCE,
            "OpenSearchProjectRule")
        .withRuleFactory(OpenSearchProjectRule::new));

    protected OpenSearchProjectRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalProject project = (LogicalProject) rel;
        RelTraitSet traitSet = project.getTraitSet().replace(OpenSearchConvention.INSTANCE);

        return new OpenSearchProject(
            project.getCluster(),
            traitSet,
            convert(project.getInput(), OpenSearchConvention.INSTANCE),
            project.getProjects(),
            project.getRowType()
        );
    }
}
