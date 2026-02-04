/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.planner;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;

/**
 * Default implementation of QueryPlanner using Calcite for logical optimization.
 */
public class DefaultQueryPlanner implements QueryPlanner {

    @Override
    public RelNode optimize(RelNode logicalPlan) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE);
        builder.addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE);
        builder.addRuleInstance(CoreRules.FILTER_MERGE);
        builder.addRuleInstance(CoreRules.PROJECT_MERGE);
        builder.addRuleInstance(CoreRules.PROJECT_REMOVE);

        HepPlanner hepPlanner = new HepPlanner(builder.build());
        hepPlanner.setRoot(logicalPlan);
        return hepPlanner.findBestExp();
    }
}
