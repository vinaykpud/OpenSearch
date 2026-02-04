/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.optimizer;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.queryplanner.optimizer.rules.AggregatePartialRule;
import org.opensearch.queryplanner.optimizer.rules.OpenSearchAggregateRule;
import org.opensearch.queryplanner.optimizer.rules.OpenSearchFilterRule;
import org.opensearch.queryplanner.optimizer.rules.OpenSearchProjectRule;
import org.opensearch.queryplanner.optimizer.rules.OpenSearchSortRule;
import org.opensearch.queryplanner.optimizer.rules.OpenSearchScanRule;
import org.opensearch.queryplanner.optimizer.rules.OpenSearchNativeEngineRule;
import org.opensearch.queryplanner.physical.rel.OpenSearchConvention;
import org.opensearch.queryplanner.physical.rel.OpenSearchRel;

/**
 * Physical optimizer that converts logical plans to physical plans.
 *
 * <p>Uses Calcite's VolcanoPlanner (cost-based optimizer) to:
 * <ol>
 *   <li>Convert logical operators to physical operators (our OpenSearchRel implementations)</li>
 *   <li>Apply physical optimization rules</li>
 *   <li>Choose the lowest-cost plan</li>
 * </ol>
 *
 * <h2>Optimization Flow:</h2>
 * <pre>
 * Logical Plan (Convention.NONE)
 *        │
 *        ▼
 * VolcanoPlanner
 *   - Registers converter rules (Logical → OpenSearch)
 *   - Explores equivalent plans
 *   - Applies cost model
 *        │
 *        ▼
 * Physical Plan (OpenSearchConvention)
 * </pre>
 *
 * <h2>Usage:</h2>
 * <pre>{@code
 * PhysicalOptimizer optimizer = new PhysicalOptimizer();
 * OpenSearchRel physicalPlan = optimizer.optimize(logicalPlan);
 * ExecNode execNode = physicalPlan.toExecNode();
 * }</pre>
 */
public class PhysicalOptimizer {

    private static final Logger logger = LogManager.getLogger(PhysicalOptimizer.class);

    private final boolean distributedMode;
    private final OpenSearchNativeEngineRule engineRule;

    /**
     * Create a PhysicalOptimizer.
     *
     * @param distributedMode If true, enable distribution traits and aggregate splitting
     */
    public PhysicalOptimizer(boolean distributedMode) {
        this.distributedMode = distributedMode;
        this.engineRule = new OpenSearchNativeEngineRule(true);
    }

    /**
     * Optimize a logical plan into a physical plan.
     *
     * @param logicalPlan The logical RelNode tree (Convention.NONE)
     * @return Physical RelNode tree (OpenSearchConvention)
     */
    public RelNode optimize(RelNode logicalPlan) {
        logger.debug("Input logical plan:\n{}", safeExplain(logicalPlan));

        // Create a new VolcanoPlanner for physical optimization
        // (don't reuse the logical plan's planner which may be a HepPlanner)
        VolcanoPlanner planner = new VolcanoPlanner();

        // Register required traits
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        // Register distribution trait for distributed execution
        if (distributedMode) {
            planner.addRelTraitDef(OpenSearchDistributionTraitDef.INSTANCE);
        }

        // Register our converter rules
        registerRules(planner);

        // Create a new cluster with our planner and proper metadata providers
        org.apache.calcite.plan.RelOptCluster newCluster = org.apache.calcite.plan.RelOptCluster.create(
            planner,
            logicalPlan.getCluster().getRexBuilder()
        );

        // Copy metadata query from original cluster to ensure explain() works
        newCluster.setMetadataQuerySupplier(org.apache.calcite.rel.metadata.RelMetadataQuery::instance);

        // Copy the logical plan tree to use the new cluster/planner
        RelNode copiedPlan = copyToNewCluster(logicalPlan, newCluster);

        // Build desired traits - request OpenSearch convention
        // In distributed mode, also include distribution trait (but use ANY to let operators declare needs)
        RelTraitSet desiredTraits = newCluster.traitSet()
            .replace(OpenSearchConvention.INSTANCE);

        if (distributedMode) {
            desiredTraits = desiredTraits.replace(OpenSearchDistribution.ANY);
        }

        // Register the logical plan with the planner and request the desired traits
        planner.setRoot(planner.changeTraits(copiedPlan, desiredTraits));

        // Find the best (lowest cost) plan
        RelNode bestPlan = planner.findBestExp();

        if (!(bestPlan instanceof OpenSearchRel)) {
            throw new IllegalStateException(
                "Physical optimization failed: result is not OpenSearchRel. Got: " + bestPlan.getClass());
        }

        // In distributed mode, ensure the root has SINGLETON distribution (gathered to coordinator).
        // If the plan doesn't have SINGLETON, wrap it in a GATHER exchange.
        if (distributedMode) {
            bestPlan = ensureSingletonAtRoot(bestPlan);
        }

        // Engine optimization: wrap subtrees in PhysicalEngine for native execution
        // This uses HepPlanner (rule-based, deterministic) - NOT cost-based
        bestPlan = applyEngineOptimization(bestPlan);

        return bestPlan;
    }

    /**
     * Apply engine optimization using HepPlanner.
     *
     * <p>This is a deterministic, rule-based transformation that wraps
     * engine-capable subtrees in PhysicalEngine nodes.
     */
    private RelNode applyEngineOptimization(RelNode plan) {
        logger.debug("Applying engine optimization with rule: {}", engineRule);

        // Build HepProgram with the provided engine rule
        HepProgram program = new HepProgramBuilder()
            .addRuleInstance(engineRule)
            .build();

        // Create HepPlanner and run
        HepPlanner hepPlanner = new HepPlanner(program);
        hepPlanner.setRoot(plan);
        RelNode optimized = hepPlanner.findBestExp();

        return optimized;
    }

    /**
     * Ensure the plan root has SINGLETON distribution by wrapping in GATHER exchange if needed.
     *
     * <p>In distributed mode, all results must be gathered to the coordinator. If the optimizer
     * didn't insert an exchange (e.g., for a simple SELECT *), we add one here.
     */
    private RelNode ensureSingletonAtRoot(RelNode plan) {
        // Check if plan already has SINGLETON distribution
        org.apache.calcite.rel.RelDistribution dist = plan.getTraitSet().getTrait(
            OpenSearchDistributionTraitDef.INSTANCE);

        if (dist != null && dist.getType() == org.apache.calcite.rel.RelDistribution.Type.SINGLETON) {
            logger.debug("Plan already has SINGLETON distribution at root");
            return plan;
        }

        // Check if root is already an Exchange (shouldn't wrap exchange in exchange)
        if (plan instanceof org.opensearch.queryplanner.physical.rel.OpenSearchExchange) {
            logger.debug("Plan root is already an Exchange");
            return plan;
        }

        logger.debug("Wrapping plan in GATHER exchange to ensure SINGLETON at root");

        // Create new trait set with SINGLETON distribution
        RelTraitSet singletonTraits = plan.getTraitSet().replace(OpenSearchDistribution.SINGLETON);

        // Wrap in GATHER exchange
        return org.opensearch.queryplanner.physical.rel.OpenSearchExchange.gather(
            plan.getCluster(),
            singletonTraits,
            plan
        );
    }

    /**
     * Safely get plan string representation, falling back to toString if explain() fails.
     */
    private String safeExplain(RelNode plan) {
        try {
            return RelOptUtil.dumpPlan("", plan, SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
        } catch (Exception e) {
            logger.trace("dumpPlan() failed, using toString: {}", e.getMessage());
            return plan.toString();
        }
    }

    /**
     * Copy a RelNode tree to use a new cluster (and thus a new planner).
     */
    private RelNode copyToNewCluster(RelNode node, org.apache.calcite.plan.RelOptCluster newCluster) {
        // Deep copy the tree with the new cluster
        return node.accept(new RelCopyShuttle(newCluster));
    }

    /**
     * RelShuttle that copies nodes to a new cluster.
     */
    private static class RelCopyShuttle extends org.apache.calcite.rel.RelShuttleImpl {
        private final org.apache.calcite.plan.RelOptCluster newCluster;

        RelCopyShuttle(org.apache.calcite.plan.RelOptCluster newCluster) {
            this.newCluster = newCluster;
        }

        @Override
        public RelNode visit(org.apache.calcite.rel.core.TableScan scan) {
            return new org.apache.calcite.rel.logical.LogicalTableScan(
                newCluster,
                newCluster.traitSet(),
                java.util.List.of(),
                scan.getTable()
            );
        }

        @Override
        public RelNode visit(org.apache.calcite.rel.logical.LogicalFilter filter) {
            RelNode newInput = filter.getInput().accept(this);
            return org.apache.calcite.rel.logical.LogicalFilter.create(
                newInput,
                filter.getCondition()
            );
        }

        @Override
        public RelNode visit(org.apache.calcite.rel.logical.LogicalProject project) {
            RelNode newInput = project.getInput().accept(this);
            return org.apache.calcite.rel.logical.LogicalProject.create(
                newInput,
                project.getHints(),
                project.getProjects(),
                project.getRowType()
            );
        }

        @Override
        public RelNode visit(org.apache.calcite.rel.logical.LogicalAggregate aggregate) {
            RelNode newInput = aggregate.getInput().accept(this);
            return org.apache.calcite.rel.logical.LogicalAggregate.create(
                newInput,
                aggregate.getHints(),
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                aggregate.getAggCallList()
            );
        }

        @Override
        public RelNode visit(org.apache.calcite.rel.logical.LogicalSort sort) {
            RelNode newInput = sort.getInput().accept(this);
            return org.apache.calcite.rel.logical.LogicalSort.create(
                newInput,
                sort.getCollation(),
                sort.offset,
                sort.fetch
            );
        }

        @Override
        public RelNode visit(org.apache.calcite.rel.logical.LogicalJoin join) {
            RelNode newLeft = join.getLeft().accept(this);
            RelNode newRight = join.getRight().accept(this);
            return org.apache.calcite.rel.logical.LogicalJoin.create(
                newLeft,
                newRight,
                join.getHints(),
                join.getCondition(),
                join.getVariablesSet(),
                join.getJoinType()
            );
        }

        @Override
        public RelNode visit(RelNode other) {
            // For other node types, try generic copy
            if (other instanceof org.apache.calcite.rel.SingleRel) {
                org.apache.calcite.rel.SingleRel single = (org.apache.calcite.rel.SingleRel) other;
                RelNode newInput = single.getInput().accept(this);
                return other.copy(newCluster.traitSet(), java.util.List.of(newInput));
            }
            // For leaf nodes or unhandled types, just return as-is
            // (this may cause issues if the node is truly incompatible)
            return other.copy(newCluster.traitSet(), other.getInputs());
        }
    }

    /**
     * Register all converter and optimization rules with the planner.
     */
    private void registerRules(RelOptPlanner planner) {
        // Converter rules: Logical → OpenSearch
        planner.addRule(OpenSearchScanRule.INSTANCE);
        planner.addRule(OpenSearchFilterRule.INSTANCE);
        planner.addRule(OpenSearchProjectRule.INSTANCE);
        planner.addRule(OpenSearchAggregateRule.INSTANCE);
        planner.addRule(OpenSearchSortRule.INSTANCE);

        int ruleCount = 5;

        // Distributed execution rules
        if (distributedMode) {
            // Split aggregates into PARTIAL (shards) + FINAL (coordinator)
            planner.addRule(AggregatePartialRule.INSTANCE);
            ruleCount++;
        }

        // TODO: Add physical optimization rules
        // - Filter pushdown into scan
        // - Project pushdown into scan

        logger.debug("Registered {} optimization rules (distributed={})", ruleCount, distributedMode);
    }
}
