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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.opensearch.queryplanner.physical.exec.ExecFilter;
import org.opensearch.queryplanner.physical.exec.ExecNode;

/**
 * Physical filter operator for OpenSearch.
 *
 * <p>Filters rows based on a predicate expression. This operator is used
 * when the filter cannot be pushed down to the scan (e.g., complex expressions,
 * non-indexed fields).
 *
 * <h2>Filter Pushdown:</h2>
 * <p>When possible, filters should be pushed into {@link OpenSearchScan}
 * for Lucene-level execution. This operator handles residual predicates that
 * must be evaluated after scanning.
 *
 * <h2>Example:</h2>
 * <pre>
 * SELECT * FROM orders WHERE status = 'active' AND complex_function(price) > 100
 *
 * Plan:
 *   OpenSearchFilter(condition=[complex_function(price) > 100])  ← residual
 *     OpenSearchScan(filter=[status = 'active'])            ← pushed
 * </pre>
 */
public class OpenSearchFilter extends Filter implements OpenSearchRel {

    public OpenSearchFilter(RelOptCluster cluster, RelTraitSet traitSet,
                            RelNode input, RexNode condition) {
        super(cluster, traitSet.replace(OpenSearchConvention.INSTANCE), input, condition);
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new OpenSearchFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(this);
        // Filter has CPU cost proportional to input rows
        double inputRows = mq.getRowCount(getInput());
        double cpu = inputRows;  // Evaluate condition for each input row
        double io = 0;           // No additional I/O

        return planner.getCostFactory().makeCost(rowCount, cpu, io);
    }

    @Override
    public ExecNode toExecNode() {
        // Convert child first
        OpenSearchRel childRel = (OpenSearchRel) getInput();
        ExecNode childExec = childRel.toExecNode();

        // Convert filter condition to string representation
        // TODO: Use proper expression serialization
        String filterExpr = getCondition().toString();

        return new ExecFilter(childExec, filterExpr);
    }
}
