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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.opensearch.queryplanner.physical.exec.ExecNode;
import org.opensearch.queryplanner.physical.exec.ExecProject;

import java.util.ArrayList;
import java.util.List;

/**
 * Physical projection operator for OpenSearch.
 *
 * <p>Selects and transforms columns from its input. Supports:
 * <ul>
 *   <li>Column selection (simple field references)</li>
 *   <li>Expression evaluation (computed columns)</li>
 *   <li>Column renaming</li>
 * </ul>
 *
 * <h2>Optimization:</h2>
 * <p>Simple projections (just column selection, no expressions) can often
 * be pushed into the scan operator for column pruning.
 */
public class OpenSearchProject extends Project implements OpenSearchRel {

    public OpenSearchProject(RelOptCluster cluster, RelTraitSet traitSet,
                              RelNode input, List<? extends RexNode> projects,
                              RelDataType rowType) {
        super(cluster, traitSet.replace(OpenSearchConvention.INSTANCE),
            input, projects, rowType);
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input,
                        List<RexNode> projects, RelDataType rowType) {
        return new OpenSearchProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(this);
        // CPU cost depends on whether we have expressions to evaluate
        double cpu = rowCount * getProjects().size() * 0.1;  // Small cost per column
        double io = 0;

        return planner.getCostFactory().makeCost(rowCount, cpu, io);
    }

    @Override
    public ExecNode toExecNode() {
        // Convert child first
        OpenSearchRel childRel = (OpenSearchRel) getInput();
        ExecNode childExec = childRel.toExecNode();

        // Extract column names from projections
        List<String> columns = new ArrayList<>();
        RelDataType inputRowType = getInput().getRowType();

        for (RexNode project : getProjects()) {
            if (project instanceof RexInputRef) {
                // Simple column reference
                RexInputRef ref = (RexInputRef) project;
                RelDataTypeField field = inputRowType.getFieldList().get(ref.getIndex());
                columns.add(field.getName());
            } else {
                // Expression - use output field name
                // TODO: Handle expressions properly
                int index = columns.size();
                RelDataTypeField outputField = getRowType().getFieldList().get(index);
                columns.add(outputField.getName());
            }
        }

        return new ExecProject(childExec, columns);
    }

    /**
     * Check if this is a simple projection (only column references, no expressions).
     */
    public boolean isSimpleProjection() {
        for (RexNode project : getProjects()) {
            if (!(project instanceof RexInputRef)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get the list of column names being projected.
     */
    public List<String> getProjectedColumnNames() {
        List<String> columns = new ArrayList<>();
        for (RelDataTypeField field : getRowType().getFieldList()) {
            columns.add(field.getName());
        }
        return columns;
    }
}
