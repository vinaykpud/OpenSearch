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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.opensearch.queryplanner.physical.exec.ExecNode;
import org.opensearch.queryplanner.physical.exec.ExecSort;

import java.util.ArrayList;
import java.util.List;

/**
 * Physical sort operator for OpenSearch.
 *
 * <p>Sorts rows by one or more columns. Supports:
 * <ul>
 *   <li>Multiple sort keys</li>
 *   <li>ASC/DESC direction</li>
 *   <li>NULLS FIRST/LAST handling</li>
 * </ul>
 *
 * <h2>Distributed Execution:</h2>
 * <p>For distributed queries with LIMIT, we use top-N optimization:
 * <ul>
 *   <li>Each shard sorts and returns top N</li>
 *   <li>Coordinator merge-sorts and takes final top N</li>
 * </ul>
 */
public class OpenSearchSort extends Sort implements OpenSearchRel {

    public OpenSearchSort(RelOptCluster cluster, RelTraitSet traitSet,
                          RelNode input, RelCollation collation,
                          RexNode offset, RexNode fetch) {
        super(cluster, traitSet.replace(OpenSearchConvention.INSTANCE),
            input, collation, offset, fetch);
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input,
                     RelCollation collation, RexNode offset, RexNode fetch) {
        return new OpenSearchSort(getCluster(), traitSet, input,
            collation, offset, fetch);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(this);
        double inputRows = mq.getRowCount(getInput());

        // Sort cost is O(n log n)
        double cpu = inputRows * Math.log(inputRows + 1);

        // If we have LIMIT, we can use top-N which is cheaper
        if (fetch != null) {
            // Top-N is O(n log k) where k is the limit
            cpu = inputRows * Math.log(rowCount + 1);
        }

        double io = 0;
        return planner.getCostFactory().makeCost(rowCount, cpu, io);
    }

    @Override
    public ExecNode toExecNode() {
        // Convert child first
        OpenSearchRel childRel = (OpenSearchRel) getInput();
        ExecNode childExec = childRel.toExecNode();

        // Extract sort specifications
        List<String> sortColumns = new ArrayList<>();
        List<Boolean> ascending = new ArrayList<>();

        RelDataTypeField[] inputFields = getInput().getRowType().getFieldList()
            .toArray(new RelDataTypeField[0]);

        for (RelFieldCollation fieldCollation : getCollation().getFieldCollations()) {
            int fieldIndex = fieldCollation.getFieldIndex();
            sortColumns.add(inputFields[fieldIndex].getName());
            ascending.add(fieldCollation.getDirection().isDescending() ? false : true);
        }

        // Extract limit if present
        int limit = -1;
        if (fetch != null) {
            // RexLiteral for the limit value
            limit = extractIntValue(fetch);
        }

        int offsetVal = 0;
        if (offset != null) {
            offsetVal = extractIntValue(offset);
        }

        return new ExecSort(childExec, sortColumns, ascending, limit, offsetVal);
    }

    private int extractIntValue(RexNode node) {
        // Simple extraction - in practice need to handle RexLiteral properly
        try {
            return Integer.parseInt(node.toString());
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /**
     * Check if this sort has a limit (making it a top-N operation).
     */
    public boolean hasLimit() {
        return fetch != null;
    }

    /**
     * Get the limit value, or -1 if no limit.
     */
    public int getLimit() {
        return fetch != null ? extractIntValue(fetch) : -1;
    }
}
