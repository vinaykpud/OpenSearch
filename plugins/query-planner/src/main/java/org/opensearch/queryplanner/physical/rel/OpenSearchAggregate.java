/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.physical.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.queryplanner.optimizer.OpenSearchDistribution;
import org.opensearch.queryplanner.optimizer.OpenSearchDistributionTraitDef;
import org.opensearch.queryplanner.physical.exec.ExecAggregate;
import org.opensearch.queryplanner.physical.exec.ExecNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Physical aggregation operator for OpenSearch.
 *
 * <p>Performs grouping and aggregation functions (SUM, COUNT, AVG, MIN, MAX).
 * In distributed execution, aggregations are split into:
 * <ul>
 *   <li>PARTIAL - Computed on each shard</li>
 *   <li>FINAL - Merged at coordinator</li>
 * </ul>
 *
 * <h2>Example:</h2>
 * <pre>
 * SELECT category, SUM(price), COUNT(*) FROM orders GROUP BY category
 *
 * Distributed Plan:
 *   OpenSearchAggregate(mode=FINAL, group=[category], aggs=[SUM, COUNT])
 *     Exchange(GATHER)
 *       OpenSearchAggregate(mode=PARTIAL, group=[category], aggs=[SUM, COUNT])
 *         OpenSearchScan(orders)
 * </pre>
 */
public class OpenSearchAggregate extends Aggregate implements OpenSearchRel {

    private static final Logger logger = LogManager.getLogger(OpenSearchAggregate.class);

    private final ExecAggregate.AggMode mode;

    public OpenSearchAggregate(RelOptCluster cluster, RelTraitSet traitSet,
                                RelNode input, ImmutableBitSet groupSet,
                                List<ImmutableBitSet> groupSets,
                                List<AggregateCall> aggCalls) {
        this(cluster, traitSet, input, groupSet, groupSets, aggCalls,
            ExecAggregate.AggMode.FULL);
    }

    public OpenSearchAggregate(RelOptCluster cluster, RelTraitSet traitSet,
                                RelNode input, ImmutableBitSet groupSet,
                                List<ImmutableBitSet> groupSets,
                                List<AggregateCall> aggCalls,
                                ExecAggregate.AggMode mode) {
        super(cluster, computeTraitSet(cluster, traitSet, input, mode),
            List.of(), input, groupSet, groupSets, aggCalls);
        this.mode = mode;
    }

    /**
     * Compute trait set based on aggregation mode.
     *
     * <ul>
     *   <li>FINAL: requires SINGLETON (all partials gathered to coordinator)</li>
     *   <li>PARTIAL: preserves input distribution (runs on shards)</li>
     *   <li>FULL: single-node execution, preserves input distribution</li>
     * </ul>
     */
    private static RelTraitSet computeTraitSet(RelOptCluster cluster, RelTraitSet traitSet,
                                                RelNode input, ExecAggregate.AggMode mode) {
        RelTraitSet result = traitSet.replace(OpenSearchConvention.INSTANCE);

        if (mode == ExecAggregate.AggMode.FINAL) {
            // Final aggregate requires all partial results on one node
            result = result.replace(OpenSearchDistribution.SINGLETON);
        } else {
            // Partial or Full: preserve input distribution
            RelDistribution inputDist = input.getTraitSet()
                .getTrait(OpenSearchDistributionTraitDef.INSTANCE);
            if (inputDist != null) {
                result = result.replace(inputDist);
            }
        }

        return result;
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input,
                          ImmutableBitSet groupSet,
                          List<ImmutableBitSet> groupSets,
                          List<AggregateCall> aggCalls) {
        return new OpenSearchAggregate(getCluster(), traitSet, input,
            groupSet, groupSets, aggCalls, mode);
    }

    /**
     * Create a partial aggregation (runs on shards).
     */
    public OpenSearchAggregate asPartial() {
        return new OpenSearchAggregate(getCluster(), getTraitSet(), getInput(),
            getGroupSet(), getGroupSets(), getAggCallList(),
            ExecAggregate.AggMode.PARTIAL);
    }

    /**
     * Create a final aggregation (runs on coordinator).
     */
    public OpenSearchAggregate asFinal() {
        return new OpenSearchAggregate(getCluster(), getTraitSet(), getInput(),
            getGroupSet(), getGroupSets(), getAggCallList(),
            ExecAggregate.AggMode.FINAL);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // In distributed mode, FULL aggregates are invalid - they must be split into PARTIAL + FINAL.
        // A FULL aggregate on distributed data would only see part of the data and produce
        // incorrect results. Only a FULL aggregate with SINGLETON distribution (all data on one node)
        // can produce correct results.
        // Force the planner to choose the split version by returning huge cost for non-SINGLETON.
        if (mode == ExecAggregate.AggMode.FULL) {
            RelDistribution dist = getTraitSet().getTrait(OpenSearchDistributionTraitDef.INSTANCE);
            logger.debug("computeSelfCost: FULL aggregate with distribution={}", dist);
            // FULL aggregate is only valid if distribution is SINGLETON
            // For ANY, RANDOM, or any other distribution, return huge cost
            if (dist == null || dist.getType() != RelDistribution.Type.SINGLETON) {
                logger.debug("computeSelfCost: returning HUGE cost for FULL aggregate without SINGLETON");
                return planner.getCostFactory().makeHugeCost();
            }
        }

        double rowCount = mq.getRowCount(this);
        double inputRows = mq.getRowCount(getInput());
        // Aggregation has significant CPU cost
        double cpu = inputRows * (1 + getAggCallList().size() * 0.5);
        double io = 0;

        return planner.getCostFactory().makeCost(rowCount, cpu, io);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("mode", mode);
        return pw;
    }

    @Override
    public ExecNode toExecNode() {
        // Convert child first
        OpenSearchRel childRel = (OpenSearchRel) getInput();
        ExecNode childExec = childRel.toExecNode();

        // Extract group by columns
        List<String> groupByColumns = new ArrayList<>();
        RelDataTypeField[] inputFields = getInput().getRowType().getFieldList()
            .toArray(new RelDataTypeField[0]);

        for (int idx : getGroupSet()) {
            groupByColumns.add(inputFields[idx].getName());
        }

        // Extract aggregate functions with their output column names
        List<String> aggregates = new ArrayList<>();
        List<RelDataTypeField> outputFields = getRowType().getFieldList();
        int aggIndex = 0;
        for (AggregateCall call : getAggCallList()) {
            StringBuilder sb = new StringBuilder();
            sb.append(call.getAggregation().getName());
            sb.append("(");
            if (call.getArgList().isEmpty()) {
                sb.append("*");
            } else {
                boolean first = true;
                for (int arg : call.getArgList()) {
                    if (!first) sb.append(", ");
                    sb.append(inputFields[arg].getName());
                    first = false;
                }
            }
            sb.append(")");
            // Add the output column name (alias) so AggregateOperator uses consistent naming
            String outputName = outputFields.get(groupByColumns.size() + aggIndex).getName();
            sb.append(" AS ").append(outputName);
            aggregates.add(sb.toString());
            aggIndex++;
        }

        return new ExecAggregate(childExec, groupByColumns, aggregates, mode);
    }

    /**
     * Get the aggregation mode.
     */
    public ExecAggregate.AggMode getMode() {
        return mode;
    }
}
