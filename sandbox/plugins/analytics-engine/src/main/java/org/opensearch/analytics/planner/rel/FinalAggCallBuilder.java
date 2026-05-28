/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AggregateFunction.IntermediateField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builds FINAL aggCalls bound against PARTIAL's output schema: rebases each argList to
 * {@code groupCount + i}, applies the COUNT→SUM-style reducer swap, and pins return types
 * for engine-native merges. Pure helper — no rel-tree walking.
 *
 * <p>Called by the split rule (so Volcano's {@code typeMatchesInferred} passes) and again by
 * {@code DistributedAggregateRewriter} when post-Volcano input adapters change the chain.
 * Idempotent across both calls as long as the same {@link #classify} result is reused.
 *
 * @opensearch.internal
 */
public final class FinalAggCallBuilder {

    private FinalAggCallBuilder() {}

    /**
     * Returns the {@link CallDecomposition} per call (parallel to {@code originalCalls}). Run
     * on the ORIGINAL aggCalls before any function swap — classifying a post-swap call could
     * pick the wrong reducer.
     */
    public static List<CallDecomposition> classify(List<AggregateCall> originalCalls) {
        List<CallDecomposition> result = new ArrayList<>(originalCalls.size());
        for (AggregateCall call : originalCalls) {
            List<IntermediateField> fields = AggregateFunction.fromSqlAggFunction(call.getAggregation()).intermediateFields();
            if (fields == null) {
                result.add(CallDecomposition.PASS_THROUGH);
            } else if (fields.size() == 1) {
                result.add(CallDecomposition.of(fields.getFirst()));
            } else {
                // Aggregates with multiple intermediate fields (AVG, STDDEV, ...) must be reduced
                // to primitives before reaching FINAL.
                throw new IllegalStateException(
                    "Aggregate [" + call.getAggregation().getName() + "] has multiple intermediate fields; expected at most one"
                );
            }
        }
        return List.copyOf(result);
    }

    /** Overload for callers without literal-arg columns to forward (the common case). */
    public static List<AggregateCall> buildFinalCalls(
        List<AggregateCall> calls,
        List<CallDecomposition> decompositions,
        int groupCount,
        RelNode finalInput,
        boolean hasEmptyGroup
    ) {
        return buildFinalCalls(calls, decompositions, groupCount, finalInput, hasEmptyGroup, Map.of());
    }

    /**
     * Rebuilds FINAL aggCalls bound against {@code finalInput}. Each call's argList becomes
     * {@code [groupCount + i, ...extras]}, the function is swapped per its
     * {@link IntermediateField#reducer}, and the return type is re-inferred or pinned per the
     * engine-native-merge rules.
     */
    public static List<AggregateCall> buildFinalCalls(
        List<AggregateCall> calls,
        List<CallDecomposition> decompositions,
        int groupCount,
        RelNode finalInput,
        boolean hasEmptyGroup,
        Map<Integer, List<Integer>> extraLiteralColIdxByCallIdx
    ) {
        List<AggregateCall> rebuilt = new ArrayList<>(calls.size());
        for (int i = 0; i < calls.size(); i++) {
            AggregateCall call = calls.get(i);
            int stateColIdx = groupCount + i;
            String name = finalInput.getRowType().getFieldList().get(stateColIdx).getName();
            List<Integer> extraColIdxs = extraLiteralColIdxByCallIdx.getOrDefault(i, List.of());
            rebuilt.add(buildOne(call, decompositions.get(i), stateColIdx, extraColIdxs, name, finalInput, hasEmptyGroup));
        }
        return rebuilt;
    }

    private static AggregateCall buildOne(
        AggregateCall call,
        CallDecomposition decomposition,
        int finalArgIdx,
        List<Integer> extraColIdxs,
        String name,
        RelNode finalInput,
        boolean hasEmptyGroup
    ) {
        SqlAggFunction aggFunc;
        RelDataType explicitType;
        if (!decomposition.isDecomposed()) {
            // No decomposition known — pass the call through and let Calcite re-infer.
            aggFunc = call.getAggregation();
            explicitType = null;
        } else {
            IntermediateField field = decomposition.field();
            AggregateFunction enumFn = AggregateFunction.fromSqlAggFunction(call.getAggregation());
            boolean isEngineNativeMerge = field.reducer() == enumFn;
            if (isEngineNativeMerge) {
                // FINAL re-aggregates with the same function. STATE_EXPANDING pins to the state
                // column type; everything else keeps the user-facing return type (e.g. HLL → BIGINT).
                aggFunc = call.getAggregation();
                explicitType = enumFn.getType() == AggregateFunction.Type.STATE_EXPANDING
                    ? finalInput.getRowType().getFieldList().get(finalArgIdx).getType()
                    : call.getType();
            } else {
                // Function swap (COUNT → SUM, etc.). Let Calcite re-infer the return type.
                aggFunc = field.reducer().toSqlAggFunction();
                explicitType = null;
            }
        }

        // State column followed by any forwarded literal-arg columns.
        List<Integer> argList = new ArrayList<>(1 + extraColIdxs.size());
        argList.add(finalArgIdx);
        argList.addAll(extraColIdxs);

        return AggregateCall.create(
            aggFunc,
            call.isDistinct(),
            call.isApproximate(),
            call.ignoreNulls(),
            call.rexList,
            List.copyOf(argList),
            call.filterArg,
            call.distinctKeys,
            call.collation,
            hasEmptyGroup,
            finalInput,
            explicitType,
            name
        );
    }
}
