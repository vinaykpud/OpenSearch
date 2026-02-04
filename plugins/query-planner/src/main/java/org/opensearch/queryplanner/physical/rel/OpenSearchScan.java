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
import org.opensearch.queryplanner.OpenSearchSchemaFactory.OpenSearchTable;
import org.opensearch.queryplanner.optimizer.OpenSearchDistribution;
import org.opensearch.queryplanner.physical.exec.ExecNode;
import org.opensearch.queryplanner.physical.exec.ExecScan;

import java.util.ArrayList;
import java.util.List;

/**
 * Physical scan operator for OpenSearch indices using Lucene doc values.
 *
 * <p>This scan reads data from Lucene indices via doc_values. It is always
 * treated as a "boundary" node for native engine execution - the engine cannot
 * read Lucene directly, so OpenSearch reads and provides Arrow data to the engine.
 *
 * <p>For native/columnar scans (e.g., Parquet), use {@link OpenSearchNativeScan} instead.
 *
 * <h2>Features:</h2>
 * <ul>
 *   <li>Column pruning - only read requested columns</li>
 *   <li>Filter pushdown - Lucene query for pushed predicates</li>
 *   <li>Metadata access - index name, shard count, doc_values info</li>
 * </ul>
 */
public class OpenSearchScan extends TableScan implements OpenSearchRel {

    /**
     * Columns to project (null means all columns).
     */
    private final List<String> projects;

    /**
     * Filter to push down to Lucene (null means no filter).
     */
    private final RexNode pushedFilter;

    /**
     * Create a new OpenSearchScan.
     */
    public OpenSearchScan(RelOptCluster cluster, RelTraitSet traitSet,
                                RelOptTable table) {
        this(cluster, traitSet, table, null, null);
    }

    /**
     * Create a new OpenSearchScan with projections and filter.
     *
     * <p>The scan produces RANDOM distribution - data is spread across shards
     * without any specific partitioning scheme.
     */
    public OpenSearchScan(RelOptCluster cluster, RelTraitSet traitSet,
                                RelOptTable table, List<String> projects,
                                RexNode pushedFilter) {
        super(cluster, traitSet
            .replace(OpenSearchConvention.INSTANCE)
            .replace(OpenSearchDistribution.RANDOM),  // Data spread across shards
            table);
        this.projects = projects;
        this.pushedFilter = pushedFilter;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new OpenSearchScan(getCluster(), traitSet, table, projects, pushedFilter);
    }

    /**
     * Create a copy with column projections.
     */
    public OpenSearchScan withProjects(List<String> newProjects) {
        return new OpenSearchScan(getCluster(), getTraitSet(), table,
            newProjects, pushedFilter);
    }

    /**
     * Create a copy with pushed filter.
     */
    public OpenSearchScan withFilter(RexNode filter) {
        return new OpenSearchScan(getCluster(), getTraitSet(), table,
            projects, filter);
    }

    @Override
    public RelDataType deriveRowType() {
        if (projects == null || projects.isEmpty()) {
            return table.getRowType();
        }

        // Build row type with only projected columns
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
        // Base cost from table scan
        double rowCount = mq.getRowCount(this);
        double cpu = rowCount;
        double io = rowCount;

        // Reduce cost if we have column pruning
        if (projects != null && !projects.isEmpty()) {
            double selectivity = (double) projects.size() / table.getRowType().getFieldCount();
            io *= selectivity;
        }

        // Reduce cost significantly if we have filter pushdown
        if (pushedFilter != null) {
            // Assume filter reduces rows by 10x (rough estimate)
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
        if (pushedFilter != null) {
            pw.item("filter", pushedFilter);
        }
        return pw;
    }

    @Override
    public ExecNode toExecNode() {
        String indexName = getIndexName();
        List<String> columns = getProjectedColumns();
        String filterExpr = pushedFilter != null ? pushedFilter.toString() : null;

        return new ExecScan(indexName, columns, filterExpr);
    }

    /**
     * Get the OpenSearch index name.
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
        // Return all column names
        List<String> allColumns = new ArrayList<>();
        for (RelDataTypeField field : table.getRowType().getFieldList()) {
            allColumns.add(field.getName());
        }
        return allColumns;
    }

    /**
     * Get the pushed filter expression.
     */
    public RexNode getPushedFilter() {
        return pushedFilter;
    }

    /**
     * Check if a column has doc_values (for efficient columnar reads).
     */
    public boolean columnHasDocValues(String columnName) {
        OpenSearchTable osTable = table.unwrap(OpenSearchTable.class);
        if (osTable != null) {
            return osTable.hasDocValues(columnName);
        }
        return true;  // Assume yes if we can't check
    }

    /**
     * Get the number of primary shards.
     */
    public int getNumberOfShards() {
        OpenSearchTable osTable = table.unwrap(OpenSearchTable.class);
        if (osTable != null) {
            return osTable.getNumberOfShards();
        }
        return 1;
    }
}
