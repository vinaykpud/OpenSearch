/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.dsl.pipeline.ConversionContext;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a {@link BoolQueryBuilder} to a Calcite RexNode.
 *
 * Handles all four boolean clauses: must (AND), filter (AND), should (OR), must_not (AND NOT).
 * Uses the Composite pattern: delegates child query conversion back to the
 * {@link QueryRegistry}, allowing recursive composition of any
 * registered query types.
 */
public class BoolQueryTranslator implements QueryTranslator {

    private final QueryRegistry registry;

    /**
     * Creates a new bool query translator.
     *
     * @param registry the query translator registry for recursive child conversion
     */
    public BoolQueryTranslator(QueryRegistry registry) {
        this.registry = registry;
    }

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return BoolQueryBuilder.class;
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;
        RexBuilder rexBuilder = ctx.getRexBuilder();

        List<RexNode> conditions = new ArrayList<>();

        for (QueryBuilder mustClause : boolQuery.must()) {
            RexNode condition = registry.convert(mustClause, ctx);
            if (condition != null) {
                conditions.add(condition);
            }
        }

        for (QueryBuilder filterClause : boolQuery.filter()) {
            RexNode condition = registry.convert(filterClause, ctx);
            if (condition != null) {
                conditions.add(condition);
            }
        }

        // should → OR: combine all should clauses into a single OR condition
        if (!boolQuery.should().isEmpty()) {
            List<RexNode> shouldConditions = new ArrayList<>();
            for (QueryBuilder shouldClause : boolQuery.should()) {
                RexNode condition = registry.convert(shouldClause, ctx);
                if (condition != null) {
                    shouldConditions.add(condition);
                }
            }
            if (shouldConditions.size() == 1) {
                conditions.add(shouldConditions.get(0));
            } else if (shouldConditions.size() > 1) {
                ctx.requireOperatorSupported(SqlStdOperatorTable.OR);
                List<RexNode> flatOr = flattenConditions(shouldConditions, SqlStdOperatorTable.OR);
                conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.OR, flatOr));
            }
        }

        // must_not → AND NOT: negate each must_not clause
        for (QueryBuilder mustNotClause : boolQuery.mustNot()) {
            RexNode condition = registry.convert(mustNotClause, ctx);
            if (condition != null) {
                ctx.requireOperatorSupported(SqlStdOperatorTable.NOT);
                conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.NOT, condition));
            }
        }

        // Flatten nested ANDs to satisfy Calcite's RexUtil.isFlat requirement
        List<RexNode> flattenedConditions = flattenConditions(conditions, SqlStdOperatorTable.AND);

        if (flattenedConditions.isEmpty()) {
            return rexBuilder.makeLiteral(true);
        } else if (flattenedConditions.size() == 1) {
            return flattenedConditions.get(0);
        } else {
            ctx.requireOperatorSupported(SqlStdOperatorTable.AND);
            return rexBuilder.makeCall(SqlStdOperatorTable.AND, flattenedConditions);
        }
    }

    private List<RexNode> flattenConditions(List<RexNode> conditions, org.apache.calcite.sql.SqlOperator operator) {
        List<RexNode> flattened = new ArrayList<>();
        for (RexNode condition : conditions) {
            if (condition instanceof RexCall call
                && call.getOperator() == operator) {
                flattened.addAll(call.getOperands());
            } else {
                flattened.add(condition);
            }
        }
        return flattened;
    }
}
