/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Plan-shape tests for {@link org.opensearch.analytics.planner.rel.OpenSearchAggregate}.
 *
 * <p>1-shard inputs: {@code Aggregate(SINGLE)} runs at the shard, ER above.
 * <p>Multi-shard: {@code OpenSearchAggregateSplitRule} splits into PARTIAL/FINAL with an
 * ER in between.
 */
public class AggregatePlanShapeTests extends PlanShapeTestBase {

    public void testStatsCountStar_1shard() {
        RelNode plan = makeAggregate(stubScan(mockTable("test_index", "status", "size")), countStarCall());
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape("""
            OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[SINGLE], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    public void testStatsCountStar_2shard() {
        RelNode plan = makeAggregate(stubScan(mockTable("test_index", "status", "size")), countStarCall());
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testStatsSumByKey_1shard() {
        RelNode plan = makeAggregate(stubScan(mockTable("test_index", "status", "size")), sumCall());
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape("""
            OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], mode=[SINGLE], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    public void testStatsSumByKey_2shard() {
        RelNode plan = makeAggregate(stubScan(mockTable("test_index", "status", "size")), sumCall());
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testStatsAvgByKey_2shard() {
        // AVG is decomposed during the reduce phase into SUM/COUNT plus a Project
        // computing the quotient. After split, FINAL receives reduced primitive aggs.
        // Use AggregateCall.create with null type so Calcite infers AVG's canonical
        // return type — passing an explicit type can drift from typeMatchesInferred.
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall avg = AggregateCall.create(
            SqlStdOperatorTable.AVG,
            false,
            false,
            false,
            List.of(),
            List.of(1),
            -1,
            null,
            org.apache.calcite.rel.RelCollations.EMPTY,
            1,
            scan,
            null,
            "avg_size"
        );
        RelNode plan = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(avg));
        RelNode result = runPlanner(plan, multiShardContext());
        // Project on top performs CAST(SUM(x) / COUNT()) back to AVG's declared return type.
        // COUNT here has no field operand because the inferred AVG decomposition produces a
        // bare COUNT (counts all rows in the group, equivalent to COUNT(x) when x is not nullable).
        // Skeleton: Project ← FINAL(SUM,COUNT) ← ER ← PARTIAL(SUM,COUNT) ← Scan.
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], avg_size=[ANNOTATED_PROJECT_EXPR(id=3, backends=[mock-parquet], CAST(ANNOTATED_PROJECT_EXPR(id=2, backends=[mock-parquet], /($1, $2))):INTEGER NOT NULL)], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], agg#0=[SUM($1)], agg#1=[COUNT()], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], agg#0=[SUM($1)], agg#1=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testStatsMultiCall_2shard() {
        // sum + count_star, both grouped by status. Single PARTIAL/FINAL pair carries
        // both calls.
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall sum = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "sum_size"
        );
        AggregateCall cnt = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        RelNode plan = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(sum, cnt));
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], sum_size=[SUM($1)], cnt=[COUNT()], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchAggregate(group=[{0}], sum_size=[SUM($1)], cnt=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Regression test for the count-then-sum/min/max nullability-shift bug. When COUNT is
     * placed before another aggregate in the call list and a {@code by} clause triggers
     * the multi-shard PARTIAL/FINAL split, the FINAL aggregate is constructed over PARTIAL's
     * output schema. PARTIAL's slot at index {@code groupCount + i} for COUNT is BIGINT NOT
     * NULL (COUNT is never nullable), but the original input had a nullable user column at
     * that index. Any following aggregate whose argList still references that index re-infers
     * its return type against a NOT-NULL operand, while the call's stored type is still
     * nullable from when it was typed against the original input. {@code Aggregate.<init>}'s
     * {@code typeMatchesInferred(Litmus.THROW)} fires the assertion.
     *
     * <p>Pre-fix: this test would throw {@code AssertionError: type mismatch: aggCall type:
     * INTEGER vs inferred type: INTEGER NOT NULL} during {@code OpenSearchAggregate.<init>}
     * inside {@code OpenSearchAggregateSplitRule.onMatch}.
     *
     * <p>Post-fix ({@code alignToInferredReturnTypes} in the split rule): the FINAL's
     * stored types are refreshed against {@code gathered} before construction, so the
     * assertion passes. A cast Project may wrap the FINAL to keep Volcano's row-type
     * equivalence happy when nullability tightens.
     */
    public void testStatsCountThenSumByKey_2shard() {
        // Build the call list in the order [count, sum] — the order that triggers the bug.
        // size:INTEGER is nullable in mockTable's row type (RelDataTypeFactory.Builder.add
        // defaults to nullable), so SUM(size)'s stored type is INTEGER (nullable), which
        // diverges from what re-inference yields against PARTIAL's count_partial slot.
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall cnt = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        AggregateCall sum = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "sum_size"
        );
        RelNode plan = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(cnt, sum));

        // The bug surface is "the planner throws an AssertionError during CBO". Asking the
        // planner to run end-to-end and just confirming it returns a non-null plan is the
        // tightest assertion this regression needs. We deliberately avoid asserting an exact
        // FINAL plan shape here because the post-fix tree may include a cast Project to
        // satisfy Volcano's row-type equivalence — that wrapper isn't part of the contract
        // this test guards. Other tests in this class cover canonical plan shapes.
        RelNode result = runPlanner(plan, multiShardContext());
        assertNotNull("Planner returned null plan for count-then-sum-by-key", result);
    }

    /**
     * Same [count, sum] pattern as {@link #testStatsCountThenSumByKey_2shard} but on a
     * 1-shard cluster. Demonstrates that the bug is reachable even when the chosen
     * Volcano-winning plan does NOT contain a PARTIAL/FINAL split.
     *
     * <p>{@link org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule#matches}
     * returns {@code true} for any SINGLE-mode aggregate regardless of shard count, so the
     * rule fires and {@link org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule#onMatch}
     * builds BOTH alternatives — SINGLE+SINGLETON and PARTIAL+ER+FINAL — for Volcano to
     * cost-compare. On 1 shard, SINGLE+SINGLETON wins on cost (no exchange needed), but
     * the PARTIAL+ER+FINAL alternative is still <em>constructed</em>, and the bug fires
     * during that construction inside {@code OpenSearchAggregate}'s constructor.
     *
     * <p>Pre-fix: same {@code AssertionError: type mismatch} as the 2-shard test, even
     * though the chosen winner would have been SINGLE+SINGLETON.
     *
     * <p>Post-fix: passes. The After-CBO plan shape is a SINGLE aggregate with no split,
     * proving the construction succeeded and Volcano picked the cheaper non-split path.
     */
    public void testStatsCountThenSumByKey_1shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall cnt = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        AggregateCall sum = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "sum_size"
        );
        RelNode plan = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(cnt, sum));

        RelNode result = runPlanner(plan, singleShardContext());

        // Volcano's chosen plan on 1 shard is the un-split SINGLE alternative — no
        // OpenSearchExchangeReducer / mode=[FINAL]. The split rule still ran (and built the
        // PARTIAL/FINAL alternative), but it lost on cost. Asserting this shape proves both
        // (a) the construction succeeded — pre-fix it would have thrown — and (b) the
        // single-shard winner is the cheaper non-split plan.
        assertPlanShape("""
            OpenSearchAggregate(group=[{0}], cnt=[COUNT()], sum_size=[SUM($1)], mode=[SINGLE], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }
}
