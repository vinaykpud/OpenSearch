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
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.queryplanner.physical.exec.ExecAggregate;
import org.opensearch.queryplanner.physical.exec.ExecEngine;
import org.opensearch.queryplanner.physical.exec.ExecFilter;
import org.opensearch.queryplanner.physical.exec.ExecLimit;
import org.opensearch.queryplanner.physical.exec.ExecNativeScan;
import org.opensearch.queryplanner.physical.exec.ExecNode;
import org.opensearch.queryplanner.physical.exec.ExecProject;
import org.opensearch.queryplanner.physical.exec.ExecScan;
import org.opensearch.queryplanner.physical.exec.ExecSort;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Physical operator that wraps a subtree for native engine execution.
 *
 * <p>OpenSearchNativeEngine is inserted by the engine optimization stage to mark
 * subtrees that should be executed by a native engine (e.g., DataFusion) rather
 * than the Java-based operators.
 *
 * <h2>Example:</h2>
 * <pre>
 * Before engine optimization:
 *   OpenSearchExchange(GATHER)
 *     └── OpenSearchFilter
 *           └── OpenSearchProject
 *                 └── OpenSearchScan
 *
 * After engine optimization:
 *   OpenSearchExchange(GATHER)
 *     └── OpenSearchNativeEngine
 *           └── OpenSearchFilter      ← wrapped subtree
 *                 └── OpenSearchProject
 *                       └── OpenSearchScan (boundary)
 * </pre>
 *
 * <p>Note: The input to OpenSearchNativeEngine is the boundary node (e.g., TableScan)
 * which provides data to the engine. The wrapped subtree is stored separately.
 */
public class OpenSearchNativeEngine extends SingleRel implements OpenSearchRel {

    /**
     * The subtree to execute in the engine.
     * This is the RelNode tree that will be converted to ExecNode and
     * executed by the native engine.
     */
    private final RelNode wrappedSubtree;

    /**
     * When true, scans are converted to ExecNativeScan (engine reads data directly).
     * When false, scans are converted to ExecScan (Lucene provides data via Arrow IPC).
     */
    private final boolean nativeScanEnabled;

    /**
     * Create an OpenSearchNativeEngine node with scan mode control.
     *
     * @param cluster The cluster
     * @param traitSet The trait set
     * @param input The input (boundary node that provides data)
     * @param wrappedSubtree The subtree to execute in the engine
     * @param nativeScanEnabled If true, convert scans to ExecNativeScan
     */
    public OpenSearchNativeEngine(RelOptCluster cluster, RelTraitSet traitSet,
                                   RelNode input, RelNode wrappedSubtree,
                                   boolean nativeScanEnabled) {
        super(cluster, traitSet.replace(OpenSearchConvention.INSTANCE), input);
        this.wrappedSubtree = wrappedSubtree;
        this.nativeScanEnabled = nativeScanEnabled;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchNativeEngine(getCluster(), traitSet, sole(inputs), wrappedSubtree, nativeScanEnabled);
    }

    @Override
    protected RelDataType deriveRowType() {
        // Return the row type of the wrapped subtree, not the input (scan)
        // This is important because the wrapped subtree may include aggregates, projects, etc.
        // that change the output schema
        return wrappedSubtree.getRowType();
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Engine execution is assumed to be faster than Java operators
        // Use a lower cost multiplier to prefer engine execution
        double rowCount = mq.getRowCount(this);
        double cpu = rowCount * 0.5;  // 50% of normal CPU cost
        double io = 0;

        return planner.getCostFactory().makeCost(rowCount, cpu, io);
    }

    /**
     * Get the wrapped subtree that will be executed by the engine.
     */
    public RelNode getWrappedSubtree() {
        return wrappedSubtree;
    }

    @Override
    public ExecNode toExecNode() {
        // Convert the wrapped subtree to ExecNode with scan mode awareness
        ExecNode subPlan = convertSubtreeToExecNode(wrappedSubtree);

        // Wrap in ExecEngine
        return new ExecEngine(subPlan);
    }

    /**
     * Unwrap HepRelVertex to get the underlying RelNode.
     */
    private RelNode unwrap(RelNode node) {
        if (node instanceof HepRelVertex) {
            return ((HepRelVertex) node).getCurrentRel();
        }
        return node;
    }

    /**
     * Recursively convert a RelNode tree to ExecNode tree.
     *
     * <p>When nativeScanEnabled is true, OpenSearchScan nodes are converted
     * to ExecNativeScan instead of ExecScan. This method handles the entire
     * subtree recursively, building each ExecNode type manually.
     */
    private ExecNode convertSubtreeToExecNode(RelNode node) {
        // Unwrap HepRelVertex if present
        node = unwrap(node);

        if (!(node instanceof OpenSearchRel)) {
            throw new IllegalStateException("Expected OpenSearchRel but got: " + node.getClass().getName());
        }

        // Handle scan nodes - convert to native scan when nativeScanEnabled
        if (node instanceof OpenSearchScan) {
            OpenSearchScan scan = (OpenSearchScan) node;
            if (nativeScanEnabled) {
                return new ExecNativeScan(
                    scan.getIndexName(),
                    scan.getProjectedColumns(),
                    scan.getPushedFilter() != null ? scan.getPushedFilter().toString() : null
                );
            } else {
                return new ExecScan(
                    scan.getIndexName(),
                    scan.getProjectedColumns(),
                    scan.getPushedFilter() != null ? scan.getPushedFilter().toString() : null
                );
            }
        }

        // Handle native scan nodes (already native)
        if (node instanceof OpenSearchNativeScan) {
            OpenSearchNativeScan scan = (OpenSearchNativeScan) node;
            return new ExecNativeScan(
                scan.getIndexName(),
                scan.getProjectedColumns(),
                scan.getFilter() != null ? scan.getFilter().toString() : null
            );
        }

        // Handle filter
        if (node instanceof OpenSearchFilter) {
            OpenSearchFilter filter = (OpenSearchFilter) node;
            ExecNode childExec = convertSubtreeToExecNode(unwrap(filter.getInput()));
            return new ExecFilter(childExec, filter.getCondition().toString());
        }

        // Handle project
        if (node instanceof OpenSearchProject) {
            OpenSearchProject project = (OpenSearchProject) node;
            ExecNode childExec = convertSubtreeToExecNode(unwrap(project.getInput()));
            List<String> columns = project.getRowType().getFieldNames();
            return new ExecProject(childExec, columns);
        }

        // Handle aggregate
        if (node instanceof OpenSearchAggregate) {
            OpenSearchAggregate agg = (OpenSearchAggregate) node;
            RelNode aggInput = unwrap(agg.getInput());
            ExecNode childExec = convertSubtreeToExecNode(aggInput);
            List<String> groupBy = agg.getGroupSet().asList().stream()
                .map(i -> aggInput.getRowType().getFieldNames().get(i))
                .collect(Collectors.toList());
            List<String> aggregates = agg.getAggCallList().stream()
                .map(call -> call.getAggregation().getName() + "(" +
                    (call.getArgList().isEmpty() ? "*" :
                        aggInput.getRowType().getFieldNames().get(call.getArgList().get(0))) + ")")
                .collect(Collectors.toList());
            return new ExecAggregate(childExec, groupBy, aggregates, ExecAggregate.AggMode.PARTIAL);
        }

        // Handle sort
        if (node instanceof OpenSearchSort) {
            OpenSearchSort sort = (OpenSearchSort) node;
            RelNode sortInput = unwrap(sort.getInput());
            ExecNode childExec = convertSubtreeToExecNode(sortInput);

            // Check if this is a LIMIT-only operation
            if (sort.getCollation().getFieldCollations().isEmpty() && sort.fetch != null) {
                int limit = ((Number) ((org.apache.calcite.rex.RexLiteral) sort.fetch).getValue()).intValue();
                return new ExecLimit(childExec, limit);
            }

            // It's a real sort
            List<String> sortColumns = sort.getCollation().getFieldCollations().stream()
                .map(fc -> sortInput.getRowType().getFieldNames().get(fc.getFieldIndex()))
                .collect(Collectors.toList());
            List<Boolean> ascending = sort.getCollation().getFieldCollations().stream()
                .map(fc -> fc.direction.isDescending() ? Boolean.FALSE : Boolean.TRUE)
                .collect(Collectors.toList());
            return new ExecSort(childExec, sortColumns, ascending, 0, 0);
        }

        // Fallback to standard conversion (may not handle nativeScanEnabled correctly)
        return ((OpenSearchRel) node).toExecNode();
    }

    @Override
    public String toString() {
        return "OpenSearchNativeEngine[" + wrappedSubtree.getRelTypeName() + "]";
    }
}
