/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchProject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Re-introduces a STATE_EXPANDING aggregate's literal config args (e.g. {@code take(field, 10)}'s
 * {@code 10}) as constant columns above the StageInputScan. PARTIAL emits only state, so FINAL
 * needs the literals projected back in to reference them by index.
 *
 * <p>Returns the input chain unchanged when no literals were captured by the split rule.
 *
 * @opensearch.internal
 */
final class LiteralProjectTransformer implements FinalInputTransformer {

    private static final String LITERAL_COL_PREFIX = "$lit_call";

    @Override
    public TransformResult transform(OpenSearchAggregate finalAgg, TransformResult current) {
        // Step 1: bail out fast when no literals were captured.
        Map<Integer, List<RexLiteral>> literalsByCall = finalAgg.getFinalExtraLiteralArgs();
        if (literalsByCall.isEmpty()) return current;

        // Step 2: prepare collectors for the new Project — expressions list + parallel row type.
        RelNode input = current.finalInput();
        RelDataType inputType = input.getRowType();
        RelDataTypeFactory.Builder projectType = finalAgg.getCluster().getTypeFactory().builder();
        List<RexNode> projectExpressions = new ArrayList<>(inputType.getFieldCount() + literalsByCall.size());

        // Step 3: forward every existing input column through unchanged.
        addPassThroughColumns(inputType, projectExpressions, projectType);

        // Step 4: append one constant column per captured literal; record where each call's
        // literals landed so FinalAggCallBuilder can extend the call's argList.
        Map<Integer, List<Integer>> literalColumnsByCall = addLiteralColumns(literalsByCall, projectExpressions, projectType);

        // Step 5: wrap the input chain in an OpenSearchProject carrying the assembled columns.
        OpenSearchProject project = new OpenSearchProject(
            input.getCluster(),
            input.getTraitSet(),
            input,
            projectExpressions,
            projectType.build(),
            finalAgg.getViableBackends()
        );

        // Step 6: hand the new chain plus the per-call literal indexes to the next transformer.
        return new TransformResult(project, Map.copyOf(literalColumnsByCall));
    }

    /** Forwards every input column through unchanged as a {@link RexInputRef}. */
    private static void addPassThroughColumns(
        RelDataType inputType,
        List<RexNode> projectExpressions,
        RelDataTypeFactory.Builder projectType
    ) {
        for (int idx = 0; idx < inputType.getFieldCount(); idx++) {
            RelDataType columnType = inputType.getFieldList().get(idx).getType();
            String columnName = inputType.getFieldList().get(idx).getName();
            projectExpressions.add(new RexInputRef(idx, columnType));
            projectType.add(columnName, columnType);
        }
    }

    /**
     * Appends one constant column per captured literal and records the resulting column
     * index per call so {@code FinalAggCallBuilder} can append them to each call's argList.
     */
    private static Map<Integer, List<Integer>> addLiteralColumns(
        Map<Integer, List<RexLiteral>> literalsByCall,
        List<RexNode> projectExprs,
        RelDataTypeFactory.Builder projectType
    ) {
        Map<Integer, List<Integer>> literalColumnsByCall = new LinkedHashMap<>();
        for (Map.Entry<Integer, List<RexLiteral>> entry : literalsByCall.entrySet()) {
            int callIdx = entry.getKey();
            List<RexLiteral> literals = entry.getValue();
            List<Integer> columnIdxs = new ArrayList<>(literals.size());
            for (int litIdx = 0; litIdx < literals.size(); litIdx++) {
                RexLiteral literal = literals.get(litIdx);
                // Capture the column index BEFORE adding — this is where the literal will land.
                int newColumnIdx = projectExprs.size();
                projectExprs.add(literal);
                projectType.add(literalColumnName(callIdx, litIdx), literal.getType());
                columnIdxs.add(newColumnIdx);
            }
            literalColumnsByCall.put(callIdx, List.copyOf(columnIdxs));
        }
        return literalColumnsByCall;
    }

    private static String literalColumnName(int callIdx, int litIdx) {
        return LITERAL_COL_PREFIX + callIdx + "_" + litIdx;
    }
}
