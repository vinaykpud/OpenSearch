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
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.opensearch.queryplanner.physical.exec.ExecLimit;
import org.opensearch.queryplanner.physical.exec.ExecNode;

import java.util.List;

/**
 * Physical limit operator for OpenSearch.
 *
 * <p>Limits the number of rows returned. This is a simple operator
 * that stops reading after N rows.
 *
 * <p>Note: For queries with ORDER BY + LIMIT, prefer using
 * {@link OpenSearchSort} which combines both operations for
 * more efficient top-N processing.
 */
public class OpenSearchLimit extends SingleRel implements OpenSearchRel {

    private final int limit;
    private final int offset;

    public OpenSearchLimit(RelOptCluster cluster, RelTraitSet traitSet,
                           RelNode input, int limit, int offset) {
        super(cluster, traitSet.replace(OpenSearchConvention.INSTANCE), input);
        this.limit = limit;
        this.offset = offset;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchLimit(getCluster(), traitSet, sole(inputs), limit, offset);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(this);
        // Limit is very cheap - just counting rows
        double cpu = rowCount;
        double io = 0;

        return planner.getCostFactory().makeCost(rowCount, cpu, io);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double inputRows = mq.getRowCount(getInput());
        // Output is at most 'limit' rows
        return Math.min(inputRows, limit);
    }

    @Override
    public ExecNode toExecNode() {
        // Convert child first
        OpenSearchRel childRel = (OpenSearchRel) getInput();
        ExecNode childExec = childRel.toExecNode();

        return new ExecLimit(childExec, limit);
    }

    public int getLimit() {
        return limit;
    }

    public int getOffset() {
        return offset;
    }
}
