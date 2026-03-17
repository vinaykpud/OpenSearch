/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.dsl.ConversionContext;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a {@link RangeQueryBuilder} to Calcite comparison RexNodes.
 */
public class RangeQueryTranslator implements QueryTranslator {

    /** Creates a new range query translator. */
    public RangeQueryTranslator() {}

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return RangeQueryBuilder.class;
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        RangeQueryBuilder rangeQuery = (RangeQueryBuilder) query;
        String fieldName = rangeQuery.fieldName();
        RexBuilder rexBuilder = ctx.getRexBuilder();

        RelDataTypeField field = ctx.getRowType().getField(fieldName, false, false);
        if (field == null) {
            throw new RuntimeException("Field '" + fieldName + "' not found in schema");
        }

        RexNode fieldRef = rexBuilder.makeInputRef(field.getType(), field.getIndex());

        List<RexNode> conditions = new ArrayList<>();

        if (rangeQuery.from() != null && rangeQuery.includeLower()) {
            ctx.requireOperatorSupported(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
            RexNode literal = rexBuilder.makeLiteral(rangeQuery.from(), field.getType(), true);
            conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, fieldRef, literal));
        }

        if (rangeQuery.from() != null && !rangeQuery.includeLower()) {
            ctx.requireOperatorSupported(SqlStdOperatorTable.GREATER_THAN);
            RexNode literal = rexBuilder.makeLiteral(rangeQuery.from(), field.getType(), true);
            conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, fieldRef, literal));
        }

        if (rangeQuery.to() != null && rangeQuery.includeUpper()) {
            ctx.requireOperatorSupported(SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
            RexNode literal = rexBuilder.makeLiteral(rangeQuery.to(), field.getType(), true);
            conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, fieldRef, literal));
        }

        if (rangeQuery.to() != null && !rangeQuery.includeUpper()) {
            ctx.requireOperatorSupported(SqlStdOperatorTable.LESS_THAN);
            RexNode literal = rexBuilder.makeLiteral(rangeQuery.to(), field.getType(), true);
            conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, fieldRef, literal));
        }

        if (conditions.isEmpty()) {
            return rexBuilder.makeLiteral(true);
        } else if (conditions.size() == 1) {
            return conditions.get(0);
        } else {
            ctx.requireOperatorSupported(SqlStdOperatorTable.AND);
            return rexBuilder.makeCall(SqlStdOperatorTable.AND, conditions);
        }
    }
}
