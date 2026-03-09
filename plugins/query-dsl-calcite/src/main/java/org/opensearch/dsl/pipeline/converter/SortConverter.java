/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.pipeline.converter;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.pipeline.AbstractDslConverter;
import org.opensearch.dsl.pipeline.ConversionContext;
import org.opensearch.dsl.pipeline.PipelinePhase;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts DSL sort and from/size into a single LogicalSort node
 * with collation (ordering) and offset/fetch (pagination).
 */
public class SortConverter extends AbstractDslConverter {

    /** Creates a new SortConverter for the SORT phase. */
    public SortConverter() {
        super(PipelinePhase.SORT);
    }

    @Override
    protected boolean isApplicable(ConversionContext ctx) {
        return hasSort(ctx) || hasNonDefaultPagination(ctx);
    }

    @Override
    protected void validate(ConversionContext ctx) throws ConversionException {
        ctx.requireRelNodeSupported(LogicalSort.class);
    }

    @Override
    protected RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException {
        RelCollation collation = buildCollation(input, ctx);
        RexNode offset = buildOffset(ctx);
        RexNode fetch = buildFetch(ctx);

        return LogicalSort.create(input, collation, offset, fetch);
    }

    private RelCollation buildCollation(RelNode input, ConversionContext ctx) throws ConversionException {
        if (!hasSort(ctx)) {
            return RelCollations.EMPTY;
        }

        RelDataType rowType = input.getRowType();
        List<RelFieldCollation> fieldCollations = new ArrayList<>();

        for (SortBuilder<?> sortBuilder : ctx.getSearchSource().sorts()) {
            if (sortBuilder instanceof FieldSortBuilder fieldSort) {
                String fieldName = fieldSort.getFieldName();
                RelDataTypeField field = rowType.getField(fieldName, true, false);
                if (field == null) {
                    throw ConversionException.invalidField(fieldName);
                }

                RelFieldCollation.Direction direction = (fieldSort.order() == SortOrder.ASC)
                    ? RelFieldCollation.Direction.ASCENDING
                    : RelFieldCollation.Direction.DESCENDING;

                RelFieldCollation.NullDirection nullDirection = (fieldSort.order() == SortOrder.ASC)
                    ? RelFieldCollation.NullDirection.LAST
                    : RelFieldCollation.NullDirection.FIRST;

                fieldCollations.add(new RelFieldCollation(field.getIndex(), direction, nullDirection));
            } else {
                throw new UnsupportedOperationException(
                    "Sort type not supported: " + sortBuilder.getClass().getSimpleName()
                );
            }
        }

        return RelCollations.of(fieldCollations);
    }

    private RexNode buildOffset(ConversionContext ctx) {
        SearchSourceBuilder ss = ctx.getSearchSource();
        int from = ss.from() != -1 ? ss.from() : 0;
        if (from <= 0) {
            return null;
        }
        return ctx.getRexBuilder().makeLiteral(from,
            ctx.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER), false);
    }

    private RexNode buildFetch(ConversionContext ctx) {
        if (!hasNonDefaultPagination(ctx)) {
            return null;
        }
        SearchSourceBuilder ss = ctx.getSearchSource();
        int size = ss.size() != -1 ? ss.size() : 10;
        return ctx.getRexBuilder().makeLiteral(size,
            ctx.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER), false);
    }

    private static boolean hasSort(ConversionContext ctx) {
        return ctx.getSearchSource().sorts() != null && !ctx.getSearchSource().sorts().isEmpty();
    }

    private static boolean hasNonDefaultPagination(ConversionContext ctx) {
        SearchSourceBuilder ss = ctx.getSearchSource();
        int from = ss.from() != -1 ? ss.from() : 0;
        int size = ss.size() != -1 ? ss.size() : 10;
        return !(from == 0 && size == 10);
    }
}
