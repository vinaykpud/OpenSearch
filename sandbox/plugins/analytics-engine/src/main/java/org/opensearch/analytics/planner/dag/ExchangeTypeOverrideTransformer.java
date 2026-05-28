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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.analytics.planner.rel.CallDecomposition;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;

import java.util.List;

/**
 * Re-types the {@link OpenSearchStageInputScan} when an aggregate's intermediate state has a
 * different shape than PARTIAL's declared output — e.g. APPROX_COUNT_DISTINCT emits a
 * {@code VARBINARY} HLL sketch.
 *
 * <p>Uses the per-call classification stashed on the FINAL aggregate at split-rule time;
 * never re-classifies post-swap calls.
 *
 * @opensearch.internal
 */
final class ExchangeTypeOverrideTransformer implements FinalInputTransformer {

    @Override
    public TransformResult transform(OpenSearchAggregate finalAgg, TransformResult current) {
        // Step 1: bail out fast if the input chain isn't an exchange-over-StageInputScan.
        RelNode exchange = current.finalInput();
        if (exchange.getInputs().isEmpty()) return current;
        if (!(exchange.getInputs().get(0) instanceof OpenSearchStageInputScan stageInput)) return current;

        // Step 2: bail out if there's no per-call classification to drive type resolution.
        List<CallDecomposition> decompositions = finalAgg.getDecompositions();
        if (decompositions.isEmpty()) return current;

        // Step 3: collect inputs needed to walk the StageInputScan's row type.
        RelDataTypeFactory typeFactory = finalAgg.getCluster().getTypeFactory();
        RelDataType stageInputType = stageInput.getRowType();
        int groupCount = finalAgg.getGroupSet().cardinality();

        // Step 4: rebuild the row type column-by-column, tracking whether anything changed.
        RelDataTypeFactory.Builder rebuiltType = typeFactory.builder();
        boolean changed = false;
        for (int idx = 0; idx < stageInputType.getFieldCount(); idx++) {
            RelDataTypeField column = stageInputType.getFieldList().get(idx);
            // Group-key columns sit at idx < groupCount → callIdx negative → keep declared type.
            RelDataType resolvedType = resolveColumnType(column, idx - groupCount, decompositions, typeFactory);
            if (!resolvedType.equals(column.getType())) changed = true;
            rebuiltType.add(column.getName(), resolvedType);
        }

        // Step 5: no-op fast path — every column kept its declared type.
        if (!changed) return current;

        // Step 6: rebuild the StageInputScan with the new row type and copy the exchange over it.
        OpenSearchStageInputScan rebuiltStageInput = new OpenSearchStageInputScan(
            stageInput.getCluster(),
            stageInput.getTraitSet(),
            stageInput.getChildStageId(),
            rebuiltType.build(),
            stageInput.getViableBackends()
        );
        RelNode rebuiltExchange = exchange.copy(exchange.getTraitSet(), List.of(rebuiltStageInput));

        // Step 7: hand the new chain to the next transformer; literal-column map is unchanged.
        return new TransformResult(rebuiltExchange, current.extraLiteralColIdxByCallIdx());
    }

    /**
     * Group-key columns ({@code callIdx < 0}) keep their declared type; agg-call columns
     * resolve through their {@link CallDecomposition#field()}'s typeResolver, but only if
     * the call has a decomposition (PASS_THROUGH calls keep the declared type).
     */
    private static RelDataType resolveColumnType(
        RelDataTypeField column,
        int callIdx,
        List<CallDecomposition> decompositions,
        RelDataTypeFactory typeFactory
    ) {
        if (callIdx < 0 || callIdx >= decompositions.size()) return column.getType();
        CallDecomposition decomposition = decompositions.get(callIdx);
        if (!decomposition.isDecomposed()) return column.getType();
        return decomposition.field().typeResolver().resolve(List.of(column.getType()), typeFactory);
    }
}
