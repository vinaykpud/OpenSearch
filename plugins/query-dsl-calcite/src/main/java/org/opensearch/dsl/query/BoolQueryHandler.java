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
 * Converts a {@link BoolQueryBuilder} to a Calcite AND RexNode.
 *
 * Uses the Composite pattern: delegates child query conversion back to the
 * {@link QueryHandlerRegistry}, allowing recursive composition of any
 * registered query types.
 */
public class BoolQueryHandler implements QueryHandler {

    private final QueryHandlerRegistry registry;

    public BoolQueryHandler(QueryHandlerRegistry registry) {
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

        // Flatten nested ANDs to satisfy Calcite's RexUtil.isFlat requirement
        List<RexNode> flattenedConditions = flattenAndConditions(conditions);

        if (flattenedConditions.isEmpty()) {
            return rexBuilder.makeLiteral(true);
        } else if (flattenedConditions.size() == 1) {
            return flattenedConditions.get(0);
        } else {
            ctx.requireOperatorSupported(SqlStdOperatorTable.AND);
            return rexBuilder.makeCall(SqlStdOperatorTable.AND, flattenedConditions);
        }
    }

    private List<RexNode> flattenAndConditions(List<RexNode> conditions) {
        List<RexNode> flattened = new ArrayList<>();
        for (RexNode condition : conditions) {
            if (condition instanceof RexCall call
                && call.getOperator() == SqlStdOperatorTable.AND) {
                flattened.addAll(call.getOperands());
            } else {
                flattened.add(condition);
            }
        }
        return flattened;
    }
}
