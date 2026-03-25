/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.dsl.ConversionContext;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts a {@link TermsQueryBuilder} to a Calcite IN RexNode.
 */
public class TermsQueryTranslator implements QueryTranslator {

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return TermsQueryBuilder.class;
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        ctx.requireOperatorSupported(SqlStdOperatorTable.IN);

        TermsQueryBuilder termsQuery = (TermsQueryBuilder) query;
        String fieldName = termsQuery.fieldName();
        List<?> values = termsQuery.values();

        RelDataTypeField field = ctx.getRowType().getField(fieldName, false, false);
        if (field == null) {
            throw new RuntimeException("Field '" + fieldName + "' not found in schema");
        }

        RexNode fieldRef = ctx.getRexBuilder().makeInputRef(field.getType(), field.getIndex());
        List<RexNode> literals = values.stream()
            .map(value -> ctx.getRexBuilder().makeLiteral(value, field.getType(), true))
            .collect(Collectors.toList());

        return ctx.getRexBuilder().makeIn(fieldRef, literals);
    }
}
