/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.CallDecomposition;
import org.opensearch.analytics.planner.rel.FinalAggCallBuilder;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.spi.AggregateFunction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Volcano rule that splits an {@link OpenSearchAggregate} into PARTIAL + FINAL when the input
 * is partitioned. PARTIAL carries the original aggCalls (with lossy-array repair for
 * LIST/VALUES); FINAL is built with rebased aggCalls via {@link FinalAggCallBuilder}. The
 * exchange between them is inserted automatically by the SINGLETON trait request on PARTIAL.
 *
 * <p>Any input-chain adaptation that needs to happen after Volcano runs in
 * {@code DistributedAggregateRewriter}.
 *
 * @opensearch.internal
 */
public class OpenSearchAggregateSplitRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchAggregateSplitRule(PlannerContext context) {
        super(operand(OpenSearchAggregate.class, operand(RelNode.class, any())), "OpenSearchAggregateSplitRule");
        this.context = context;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchAggregate aggregate = call.rel(0);
        return aggregate.getMode() == AggregateMode.SINGLE;
    }

    /**
     * True when PARTIAL/FINAL split would yield a malformed row type or invalid aggregate
     * semantics. In those cases {@link #onMatch} still produces the SINGLE+SINGLETON
     * alternative (so the planner can route shard input through a coordinator gather), but
     * skips the PARTIAL+ER+FINAL alternative.
     *
     * <p>Two cases are unsafe today:
     * <ul>
     *   <li><b>percentile_approx</b> is a 2-arg aggregate (field, percent) whose FINAL phase
     *       needs (tdigest_state, percent_literal). {@code AggregateDecompositionResolver}'s
     *       single-field rewrite paths only produce a single-arg FINAL call, yielding
     *       {@code "Type mismatch: rel rowtype: RecordType(BIGINT p50, BIGINT p50_0) NOT NULL,
     *       equiv rowtype: RecordType(INTEGER bucket, BIGINT p50)"}. Other aggCalls in the
     *       same Aggregate (SUM, AVG, etc.) inherit the single-stage execution.</li>
     *   <li><b>Cross-family non-prefix groupSet</b>: PARTIAL's output places group keys at
     *       positions {@code [0..groupCount)}. FINAL reuses ORIGINAL's groupSet against
     *       PARTIAL's output. When an input column at index {@code k >= groupCount} is a group
     *       key (e.g. {@code groupSet={2}, groupCount=1}), PARTIAL's output at index {@code k}
     *       is an agg-result instead, and Calcite's row-type equivalence check fires only if
     *       that agg-result's {@link SqlTypeFamily} differs from the ORIGINAL input column's
     *       family. PPL {@code timechart}'s no-{@code by} form trips this: the Project below
     *       the Aggregate keeps the raw {@code @timestamp} (DATETIME family) at position 0
     *       and materializes {@code SPAN(@timestamp)} at a later position; the agg result at
     *       that later position is {@code DOUBLE} (NUMERIC family) → cross-family mismatch
     *       ({@code "Type mismatch ... DOUBLE -> TIMESTAMP(0)"}). Same-family non-prefix
     *       cases (e.g. {@code group={1}} with both columns INTEGER + a NUMERIC agg) pass
     *       Calcite's relaxed numeric type check and don't need the skip — see
     *       {@code PlanShapeTests.testJoinWithDifferentGroupKeys_multiShard}.</li>
     * </ul>
     *
     * <p>Until {@code AggregateDecompositionResolver} gains engine-native merge support
     * (percentile_approx) and ORIGINAL→FINAL groupSet remapping (cross-family non-prefix),
     * the split is conservative in those shapes — distributed parallelism is traded for
     * correctness.
     */
    private static boolean shouldSkipPartialFinalSplit(OpenSearchAggregate aggregate) {
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            if (isPercentileApprox(aggCall)) {
                return true;
            }
        }
        int groupCount = aggregate.getGroupSet().cardinality();
        if (aggregate.getGroupSet().equals(ImmutableBitSet.range(groupCount))) {
            return false;
        }
        // Non-prefix groupSet — narrow to the cross-family case that actually trips
        // typeMatchesInferred. Each group-key index k >= groupCount would land on PARTIAL's
        // agg-output slot at (k - groupCount). If that agg's result type and the ORIGINAL
        // input column at k belong to different families, the split is unsafe.
        List<RelDataType> inputFields = aggregate.getInput().getRowType().getFieldList().stream().map(f -> f.getType()).toList();
        List<AggregateCall> aggCalls = aggregate.getAggCallList();
        for (int k : aggregate.getGroupSet().toArray()) {
            if (k < groupCount) {
                continue;
            }
            int aggIdx = k - groupCount;
            if (aggIdx >= aggCalls.size() || k >= inputFields.size()) {
                return true;  // out of bounds → split would be structurally invalid
            }
            SqlTypeFamily inputFamily = inputFields.get(k).getSqlTypeName().getFamily();
            SqlTypeFamily aggFamily = aggCalls.get(aggIdx).getType().getSqlTypeName().getFamily();
            if (inputFamily != aggFamily) {
                return true;
            }
        }
        return false;
    }

    private static boolean isPercentileApprox(AggregateCall aggCall) {
        return "PERCENTILE_APPROX".equalsIgnoreCase(aggCall.getAggregation().getName());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchAggregate aggregate = call.rel(0);
        RelNode child = call.rel(1);

        // SINGLE-on-SINGLETON alternative — wins when the child already gathers below.
        // Also the *only* alternative the rule offers when the PARTIAL/FINAL split would
        // emit a row type that fails Volcano's typeMatchesInferred — see
        // shouldSkipPartialFinalSplit for the cases.
        RelTraitSet singletonTraits = aggregate.getTraitSet().replace(context.getDistributionTraitDef().coordSingleton());
        RelNode singletonChild = convert(child, singletonTraits);
        OpenSearchAggregate singleOnSingleton = new OpenSearchAggregate(
            aggregate.getCluster(),
            singletonTraits,
            singletonChild,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggregate.getAggCallList(),
            AggregateMode.SINGLE,
            aggregate.getViableBackends(),
            aggregate.getCallAnnotations()
        );

        if (shouldSkipPartialFinalSplit(aggregate)) {
            // The PARTIAL/FINAL alternative would emit a row type that fails Volcano's
            // typeMatchesInferred check. Transform to the SINGLE+SINGLETON alternative
            // so a coordinator-side gather still satisfies a SINGLETON-demanding parent.
            call.transformTo(singleOnSingleton);
            return;
        }

        // PARTIAL + ER + FINAL alternative — wins when child is shard-partitioned.
        // Repair LIST/VALUES return type from PPL's lossy ARRAY<VARCHAR> to ARRAY<arg0> on
        // PARTIAL only, so the StageInputScan column type (and thus the FINAL substrait's
        // base_schema) matches what DataFusion's array_agg actually produces. FINAL keeps
        // the original aggCall list to satisfy Volcano's parent row-type check.
        List<AggregateCall> partialAggCalls = repairLossyReturnTypes(aggregate.getAggCallList(), child);
        RelTraitSet partialTraits = child.getTraitSet().replace(OpenSearchConvention.INSTANCE);
        OpenSearchAggregate partial = new OpenSearchAggregate(
            aggregate.getCluster(),
            partialTraits,
            child,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            partialAggCalls,
            AggregateMode.PARTIAL,
            aggregate.getViableBackends(),
            aggregate.getCallAnnotations()
        );
        RelTraitSet finalTraits = partial.getTraitSet().replace(context.getDistributionTraitDef().coordSingleton());
        RelNode gathered = convert(partial, finalTraits);
        Map<Integer, List<RexLiteral>> finalExtraLiterals = captureLiteralArgsForFinal(aggregate.getAggCallList(), child);

        // Classify ORIGINAL aggCalls once. Stashed on FINAL for post-Volcano adapters to read,
        // so they don't have to re-classify post-swap calls.
        List<CallDecomposition> decompositions = FinalAggCallBuilder.classify(aggregate.getAggCallList());

        // Build FINAL's aggCalls bound against `gathered` (PARTIAL's output schema). Required
        // up-front so Volcano's Aggregate.<init> typeMatchesInferred check passes. Any literal
        // forwarding for STATE_EXPANDING aggregates is added post-Volcano.
        List<AggregateCall> finalAggCalls = FinalAggCallBuilder.buildFinalCalls(
            aggregate.getAggCallList(),
            decompositions,
            aggregate.getGroupSet().cardinality(),
            gathered,
            aggregate.getGroupSet().isEmpty()
        );

        OpenSearchAggregate finalAggregate = new OpenSearchAggregate(
            aggregate.getCluster(),
            finalTraits,
            gathered,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            finalAggCalls,
            AggregateMode.FINAL,
            aggregate.getViableBackends(),
            aggregate.getCallAnnotations(),
            finalExtraLiterals,
            decompositions
        );

        // For empty-group queries, FINAL's output type may disagree with SINGLE's on
        // nullability (e.g. COUNT→SUM swap: COUNT is BIGINT NOT NULL, SUM-over-empty-group
        // is nullable). Wrap FINAL in a CAST-projection that restores SINGLE's row type so
        // Volcano's RelSubset equivalence check passes.
        RelNode finalAlternative = wrapWithCastIfNeeded(finalAggregate, aggregate);

        call.getPlanner().ensureRegistered(singleOnSingleton, aggregate);
        call.transformTo(finalAlternative);
    }

    /**
     * If any of {@code finalAggregate}'s output column types differs from the corresponding
     * column on {@code expected}'s row type (e.g. nullability gap from a COUNT→SUM swap),
     * wraps it in an {@link OpenSearchProject} that casts the differing columns to the
     * expected type. Returns {@code finalAggregate} unchanged when every column type matches
     * — name differences alone don't trigger a wrap.
     */
    private static RelNode wrapWithCastIfNeeded(OpenSearchAggregate finalAggregate, OpenSearchAggregate expected) {
        RelDataType actualType = finalAggregate.getRowType();
        RelDataType expectedType = expected.getRowType();
        if (!anyColumnTypeDiffers(actualType, expectedType)) return finalAggregate;

        RexBuilder rexBuilder = finalAggregate.getCluster().getRexBuilder();
        List<RexNode> projects = new ArrayList<>(actualType.getFieldCount());
        for (int idx = 0; idx < actualType.getFieldCount(); idx++) {
            RelDataType columnType = actualType.getFieldList().get(idx).getType();
            RelDataType targetType = expectedType.getFieldList().get(idx).getType();
            RexNode ref = new RexInputRef(idx, columnType);
            projects.add(columnType.equals(targetType) ? ref : rexBuilder.makeCast(targetType, ref));
        }
        return new OpenSearchProject(
            finalAggregate.getCluster(),
            finalAggregate.getTraitSet(),
            finalAggregate,
            projects,
            expectedType,
            finalAggregate.getViableBackends()
        );
    }

    private static boolean anyColumnTypeDiffers(RelDataType actual, RelDataType expected) {
        for (int idx = 0; idx < actual.getFieldCount(); idx++) {
            if (!actual.getFieldList().get(idx).getType().equals(expected.getFieldList().get(idx).getType())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Rebuild any LIST/VALUES aggCall to declare {@code ARRAY<actual-arg0>} instead of
     * PPL's lossy {@code ARRAY<VARCHAR>}. Pass-through for every other call. Used on the
     * PARTIAL side only — the FINAL keeps the original call list so Volcano's parent
     * row-type check on transformTo passes.
     */
    private static List<AggregateCall> repairLossyReturnTypes(List<AggregateCall> aggCalls, RelNode input) {
        List<AggregateCall> rebuilt = null;
        for (int i = 0; i < aggCalls.size(); i++) {
            AggregateCall call = aggCalls.get(i);
            String name = call.getAggregation().getName();
            if (!"LIST".equalsIgnoreCase(name) && !"VALUES".equalsIgnoreCase(name)) continue;
            if (call.getArgList().isEmpty()) continue;
            org.apache.calcite.rel.type.RelDataType arg0Type = input.getRowType().getFieldList().get(call.getArgList().get(0)).getType();
            org.apache.calcite.rel.type.RelDataType repaired = input.getCluster().getTypeFactory().createArrayType(arg0Type, -1);
            if (repaired.equals(call.getType())) continue;
            if (rebuilt == null) rebuilt = new ArrayList<>(aggCalls);
            rebuilt.set(
                i,
                AggregateCall.create(
                    call.getAggregation(),
                    call.isDistinct(),
                    call.isApproximate(),
                    call.ignoreNulls(),
                    call.rexList,
                    call.getArgList(),
                    call.filterArg,
                    call.distinctKeys,
                    call.collation,
                    repaired,
                    call.getName()
                )
            );
        }
        return rebuilt != null ? rebuilt : aggCalls;
    }

    private static Map<Integer, List<RexLiteral>> captureLiteralArgsForFinal(List<AggregateCall> aggCalls, RelNode child) {
        if (!(RelNodeUtils.unwrapHep(child) instanceof Project project)) {
            return Map.of();
        }
        List<RexNode> projects = project.getProjects();
        Map<Integer, List<RexLiteral>> captured = new LinkedHashMap<>();
        for (int i = 0; i < aggCalls.size(); i++) {
            AggregateCall call = aggCalls.get(i);
            AggregateFunction fn = AggregateFunction.fromSqlAggFunction(call.getAggregation());
            if (fn == null || fn.getType() != AggregateFunction.Type.STATE_EXPANDING) continue;
            List<Integer> args = call.getArgList();
            if (args.size() < 2) continue;
            // arg 0 is the value/state column; args 1+ are the configuration literals.
            List<RexLiteral> literals = new ArrayList<>(args.size() - 1);
            boolean allLiteral = true;
            for (int a = 1; a < args.size(); a++) {
                int colIdx = args.get(a);
                if (colIdx < 0 || colIdx >= projects.size() || !(projects.get(colIdx) instanceof RexLiteral lit)) {
                    allLiteral = false;
                    break;
                }
                literals.add(lit);
            }
            if (allLiteral && !literals.isEmpty()) {
                captured.put(i, List.copyOf(literals));
            }
        }
        return captured;
    }
}
