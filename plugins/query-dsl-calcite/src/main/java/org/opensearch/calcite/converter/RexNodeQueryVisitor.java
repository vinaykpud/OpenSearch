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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.calcite.exception.ConversionException;
import org.opensearch.calcite.functions.OpenSearchFunctions;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of QueryBuilderVisitor that converts OpenSearch queries to Calcite RexNode.
 *
 * RexNode represents row expressions in Calcite's logical plan, including:
 * - Field references
 * - Literals
 * - Function calls
 * - Boolean operations (AND, OR, NOT)
 * - Comparison operations (=, &lt;, &gt;, &lt;=, &gt;=)
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
        // Get field name and value
        String fieldName = query.fieldName();
        Object value = query.value();

        // Find field in row type
        RelDataTypeField field = rowType.getField(fieldName, false, false);
        if (field == null) {
            throw new RuntimeException("Field '" + fieldName + "' not found in schema");
        }

        // Create field reference
        RexNode fieldRef = rexBuilder.makeInputRef(field.getType(), field.getIndex());

        // Create literal for the value
        RexNode literal = rexBuilder.makeLiteral(value, field.getType(), true);

        // Create equality condition: field = value
        return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, fieldRef, literal);
    }

    @Override
    public RexNode visitRangeQuery(RangeQueryBuilder query) {
        // Get field name
        String fieldName = query.fieldName();

        // Find field in row type
        RelDataTypeField field = rowType.getField(fieldName, false, false);
        if (field == null) {
            throw new RuntimeException("Field '" + fieldName + "' not found in schema");
        }

        // Create field reference
        RexNode fieldRef = rexBuilder.makeInputRef(field.getType(), field.getIndex());

        // Build conditions based on range parameters
        List<RexNode> conditions = new ArrayList<>();

        // Handle gte (greater than or equal)
        if (query.from() != null && query.includeLower()) {
            RexNode literal = rexBuilder.makeLiteral(query.from(), field.getType(), true);
            conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, fieldRef, literal));
        }

        // Handle gt (greater than)
        if (query.from() != null && !query.includeLower()) {
            RexNode literal = rexBuilder.makeLiteral(query.from(), field.getType(), true);
            conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, fieldRef, literal));
        }

        // Handle lte (less than or equal)
        if (query.to() != null && query.includeUpper()) {
            RexNode literal = rexBuilder.makeLiteral(query.to(), field.getType(), true);
            conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, fieldRef, literal));
        }

        // Handle lt (less than)
        if (query.to() != null && !query.includeUpper()) {
            RexNode literal = rexBuilder.makeLiteral(query.to(), field.getType(), true);
            conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, fieldRef, literal));
        }

        // Combine conditions with AND
        if (conditions.isEmpty()) {
            // No conditions - return TRUE
            return rexBuilder.makeLiteral(true);
        } else if (conditions.size() == 1) {
            return conditions.get(0);
        } else {
            // Multiple conditions - combine with AND
            return rexBuilder.makeCall(SqlStdOperatorTable.AND, conditions);
        }
    }

    @Override
    public RexNode visitMatchQuery(MatchQueryBuilder query) {
        // Get field name and text
        String fieldName = query.fieldName();
        Object text = query.value();
        String operator = query.operator().name(); // "AND" or "OR"

        // Find field in row type
        RelDataTypeField field = rowType.getField(fieldName, false, false);
        if (field == null) {
            throw new RuntimeException("Field '" + fieldName + "' not found in schema");
        }

        // Create field reference
        RexNode fieldRef = rexBuilder.makeInputRef(field.getType(), field.getIndex());

        // Create literals for text and operator
        RexNode textLiteral = rexBuilder.makeLiteral(text.toString());
        RexNode operatorLiteral = rexBuilder.makeLiteral(operator);

        // Create MATCH_QUERY function call: MATCH_QUERY(field, text, operator)
        return rexBuilder.makeCall(
            OpenSearchFunctions.MATCH_QUERY,
            fieldRef,
            textLiteral,
            operatorLiteral
        );
    }

    @Override
    public RexNode visitBoolQuery(BoolQueryBuilder query) {
        List<RexNode> conditions = new ArrayList<>();

        // Process must clauses (required, affects scoring)
        // Convert to AND conjunction
        for (QueryBuilder mustClause : query.must()) {
            RexNode condition = visitQuery(mustClause);
            if (condition != null) {
                conditions.add(condition);
            }
        }

        // Process filter clauses (required, no scoring)
        // Convert to AND conjunction
        for (QueryBuilder filterClause : query.filter()) {
            RexNode condition = visitQuery(filterClause);
            if (condition != null) {
                conditions.add(condition);
            }
        }

        // Flatten nested AND conditions to satisfy Calcite's RexUtil.isFlat requirement
        List<RexNode> flattenedConditions = flattenAndConditions(conditions);

        // Combine all conditions with AND
        if (flattenedConditions.isEmpty()) {
            // No conditions - return TRUE
            return rexBuilder.makeLiteral(true);
        } else if (flattenedConditions.size() == 1) {
            return flattenedConditions.get(0);
        } else {
            // Multiple conditions - combine with AND
            return rexBuilder.makeCall(SqlStdOperatorTable.AND, flattenedConditions);
        }
    }

    /**
     * Flattens nested AND conditions into a single list.
     * Calcite requires that AND conditions be "flat" - no nested ANDs.
     * For example: AND(a, AND(b, c)) should be flattened to AND(a, b, c).
     *
     * @param conditions The list of conditions that may contain nested ANDs
     * @return A flattened list of conditions
     */
    private List<RexNode> flattenAndConditions(List<RexNode> conditions) {
        List<RexNode> flattened = new ArrayList<>();
        for (RexNode condition : conditions) {
            // Check if this is an AND call by checking the operator
            if (condition instanceof org.apache.calcite.rex.RexCall) {
                org.apache.calcite.rex.RexCall call = (org.apache.calcite.rex.RexCall) condition;
                if (call.getOperator() == SqlStdOperatorTable.AND) {
                    // This is an AND node - extract its operands and add them to the flattened list
                    flattened.addAll(call.getOperands());
                } else {
                    // Not an AND node - add it directly
                    flattened.add(condition);
                }
            } else {
                // Not a RexCall - add it directly
                flattened.add(condition);
            }
        }
        return flattened;
    }

    /**
     * Dispatches a QueryBuilder to the appropriate visit method.
     * This enables recursive visiting of nested queries.
     *
     * @param query The query to visit
     * @return The converted RexNode
     */
    private RexNode visitQuery(QueryBuilder query) {
        if (query instanceof BoolQueryBuilder) {
            return visitBoolQuery((BoolQueryBuilder) query);
        } else if (query instanceof TermQueryBuilder) {
            return visitTermQuery((TermQueryBuilder) query);
        } else if (query instanceof MatchQueryBuilder) {
            return visitMatchQuery((MatchQueryBuilder) query);
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
        // Match all query matches all documents
        // Represented as TRUE literal in Calcite
        return rexBuilder.makeLiteral(true);
    }
}
