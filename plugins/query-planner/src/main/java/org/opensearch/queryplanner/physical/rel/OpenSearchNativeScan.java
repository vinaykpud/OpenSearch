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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.opensearch.queryplanner.optimizer.OpenSearchDistribution;
import org.opensearch.queryplanner.physical.exec.ExecNativeScan;
import org.opensearch.queryplanner.physical.exec.ExecNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Physical scan operator for native/columnar data sources.
 *
 * <p>This scan is executed directly by the native engine (e.g., DataFusion).
 * Unlike {@link OpenSearchScan} which reads from Lucene, this scan reads from
 * columnar formats that the engine can access directly (e.g., Parquet files).
 *
 * <p>This node is <b>included</b> in the engine subtree, not treated as a boundary.
 * The engine receives the scan specification and performs the read itself.
 *
 * <h2>Usage:</h2>
 * <ul>
 *   <li>Pure columnar scan: {@code new OpenSearchNativeScan(cluster, traitSet, table)}</li>
 *   <li>Hybrid scan with filter: {@code new OpenSearchNativeScan(cluster, traitSet, table, filter)}</li>
 * </ul>
 *
 * <h2>Filter Pushdown:</h2>
 * <p>The optional filter is pushed down to the scan level, allowing the engine
 * to apply predicate pushdown during the scan (e.g., Parquet row group filtering).
 */
public class OpenSearchNativeScan extends TableScan implements OpenSearchRel {

    /**
     * Columns to project (null means all columns).
     */
    private final List<String> projects;

    /**
     * Filter to apply at scan time (null means no filter).
     * This is pushed down to the native engine for efficient filtering.
     */
    private final RexNode filter;

    /**
     * Create a native scan without filter (pure columnar scan).
     */
    public OpenSearchNativeScan(RelOptCluster cluster, RelTraitSet traitSet,
                                 RelOptTable table) {
        this(cluster, traitSet, table, null, null);
    }

    /**
     * Create a native scan with filter (hybrid scan).
     */
    public OpenSearchNativeScan(RelOptCluster cluster, RelTraitSet traitSet,
                                 RelOptTable table, RexNode filter) {
        this(cluster, traitSet, table, null, filter);
    }

    /**
     * Create a native scan with projections and optional filter.
     */
    public OpenSearchNativeScan(RelOptCluster cluster, RelTraitSet traitSet,
                                 RelOptTable table, List<String> projects,
                                 RexNode filter) {
        super(cluster, traitSet
            .replace(OpenSearchConvention.INSTANCE)
            .replace(OpenSearchDistribution.RANDOM),
            table);
        this.projects = projects;
        this.filter = filter;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new OpenSearchNativeScan(getCluster(), traitSet, table, projects, filter);
    }

    /**
     * Create a copy with column projections.
     */
    public OpenSearchNativeScan withProjects(List<String> newProjects) {
        return new OpenSearchNativeScan(getCluster(), getTraitSet(), table,
            newProjects, filter);
    }

    /**
     * Create a copy with filter.
     */
    public OpenSearchNativeScan withFilter(RexNode newFilter) {
        return new OpenSearchNativeScan(getCluster(), getTraitSet(), table,
            projects, newFilter);
    }

    @Override
    public RelDataType deriveRowType() {
        if (projects == null || projects.isEmpty()) {
            return table.getRowType();
        }

        RelDataType fullType = table.getRowType();
        var builder = getCluster().getTypeFactory().builder();

        for (String colName : projects) {
            RelDataTypeField field = fullType.getField(colName, true, false);
            if (field != null) {
                builder.add(field);
            }
        }

        return builder.build();
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(this);
        double cpu = rowCount;
        double io = rowCount;

        // Columnar scans are more efficient
        cpu *= 0.5;
        io *= 0.5;

        // Reduce cost if we have column pruning
        if (projects != null && !projects.isEmpty()) {
            double selectivity = (double) projects.size() / table.getRowType().getFieldCount();
            io *= selectivity;
        }

        // Reduce cost if we have filter pushdown
        if (filter != null) {
            cpu *= 0.1;
            io *= 0.1;
        }

        return planner.getCostFactory().makeCost(rowCount, cpu, io);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        if (projects != null && !projects.isEmpty()) {
            pw.item("projects", projects);
        }
        if (filter != null) {
            pw.item("filter", filter);
        }
        return pw;
    }

    @Override
    public ExecNode toExecNode() {
        String indexName = getIndexName();
        List<String> columns = getProjectedColumns();
        String filterExpr = filter != null ? filter.toString() : null;

        return new ExecNativeScan(indexName, columns, filterExpr);
    }

    /**
     * Get the table/index name.
     */
    public String getIndexName() {
        List<String> qualifiedName = table.getQualifiedName();
        return qualifiedName.get(qualifiedName.size() - 1);
    }

    /**
     * Get the list of columns to project.
     */
    public List<String> getProjectedColumns() {
        if (projects != null) {
            return projects;
        }
        List<String> allColumns = new ArrayList<>();
        for (RelDataTypeField field : table.getRowType().getFieldList()) {
            allColumns.add(field.getName());
        }
        return allColumns;
    }

    /**
     * Get the filter expression.
     */
    public RexNode getFilter() {
        return filter;
    }

    /**
     * Check if this scan has a filter.
     */
    public boolean hasFilter() {
        return filter != null;
    }
}
