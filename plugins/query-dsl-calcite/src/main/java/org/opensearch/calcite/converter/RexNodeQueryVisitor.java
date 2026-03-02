/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.converter;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts OpenSearch QueryBuilder instances to Calcite RexNode filter expressions.
 *
 * Each visit method translates a specific DSL query type into the equivalent
 * Calcite row expression using standard operators ({@literal =, >=, <=}, AND, etc.).
 */
public class RexNodeQueryVisitor implements QueryBuilderVisitor<RexNode> {

    private final RexBuilder rexBuilder;
    private final RelDataType rowType;

    /**
     * Creates a new RexNodeQueryVisitor.
     *
     * @param rexBuilder The RexBuilder for creating RexNode instances
     * @param rowType The row type containing field definitions
     */
    public RexNodeQueryVisitor(RexBuilder rexBuilder, RelDataType rowType) {
        this.rexBuilder = rexBuilder;
        this.rowType = rowType;
    }

    @Override
    public RexNode visitTermQuery(TermQueryBuilder query) {
        String fieldName = query.fieldName();
        Object value = query.value();

        RelDataTypeField field = rowType.getField(fieldName, false, false);
        if (field == null) {
            throw new RuntimeException("Field '" + fieldName + "' not found in schema");
        }

        RexNode fieldRef = rexBuilder.makeInputRef(field.getType(), field.getIndex());
        RexNode literal = rexBuilder.makeLiteral(value, field.getType(), true);

        return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, fieldRef, literal);
    }

    @Override
    public RexNode visitRangeQuery(RangeQueryBuilder query) {
        String fieldName = query.fieldName();

        RelDataTypeField field = rowType.getField(fieldName, false, false);
        if (field == null) {
            throw new RuntimeException("Field '" + fieldName + "' not found in schema");
        }

        RexNode fieldRef = rexBuilder.makeInputRef(field.getType(), field.getIndex());

        List<RexNode> conditions = new ArrayList<>();

        if (query.from() != null && query.includeLower()) {
            RexNode literal = rexBuilder.makeLiteral(query.from(), field.getType(), true);
            conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, fieldRef, literal));
        }

        if (query.from() != null && !query.includeLower()) {
            RexNode literal = rexBuilder.makeLiteral(query.from(), field.getType(), true);
            conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, fieldRef, literal));
        }

        if (query.to() != null && query.includeUpper()) {
            RexNode literal = rexBuilder.makeLiteral(query.to(), field.getType(), true);
            conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, fieldRef, literal));
        }

        if (query.to() != null && !query.includeUpper()) {
            RexNode literal = rexBuilder.makeLiteral(query.to(), field.getType(), true);
            conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, fieldRef, literal));
        }

        if (conditions.isEmpty()) {
            return rexBuilder.makeLiteral(true);
        } else if (conditions.size() == 1) {
            return conditions.get(0);
        } else {
            return rexBuilder.makeCall(SqlStdOperatorTable.AND, conditions);
        }
    }

    @Override
    public RexNode visitBoolQuery(BoolQueryBuilder query) {
        List<RexNode> conditions = new ArrayList<>();

        for (QueryBuilder mustClause : query.must()) {
            RexNode condition = visitQuery(mustClause);
            if (condition != null) {
                conditions.add(condition);
            }
        }

        for (QueryBuilder filterClause : query.filter()) {
            RexNode condition = visitQuery(filterClause);
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
            return rexBuilder.makeCall(SqlStdOperatorTable.AND, flattenedConditions);
        }
    }

    /**
     * Flattens nested AND conditions into a single list.
     * Calcite requires AND conditions to be flat — e.g., AND(a, AND(b, c)) becomes AND(a, b, c).
     *
     * @param conditions The list of conditions that may contain nested ANDs
     * @return A flattened list of conditions
     */
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

    /**
     * Dispatches a QueryBuilder to the appropriate visit method,
     * enabling recursive visiting of nested queries within bool clauses.
     *
     * @param query The query to visit
     * @return The converted RexNode
     */
    private RexNode visitQuery(QueryBuilder query) {
        if (query instanceof BoolQueryBuilder) {
            return visitBoolQuery((BoolQueryBuilder) query);
        } else if (query instanceof TermQueryBuilder) {
            return visitTermQuery((TermQueryBuilder) query);
        } else if (query instanceof RangeQueryBuilder) {
            return visitRangeQuery((RangeQueryBuilder) query);
        } else if (query instanceof MatchAllQueryBuilder) {
            return visitMatchAllQuery((MatchAllQueryBuilder) query);
        } else {
            throw new UnsupportedOperationException(
                "Query type not supported: " + query.getClass().getSimpleName()
            );
        }
    }

    @Override
    public RexNode visitMatchAllQuery(MatchAllQueryBuilder query) {
        return rexBuilder.makeLiteral(true);
    }
}
