/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.pipeline.clause;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.dsl.pipeline.AbstractClauseConverter;
import org.opensearch.dsl.pipeline.ConversionContext;
import org.opensearch.dsl.pipeline.PipelinePhase;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts top-level sort clauses to a LogicalSort before aggregation.
 */
public class SortConverter extends AbstractClauseConverter {

    /** Creates a new SortConverter for the SORT phase. */
    public SortConverter() {
        super(PipelinePhase.SORT);
    }

    @Override
    protected boolean isApplicable(ConversionContext ctx) {
        return ctx.getSearchSource().sorts() != null && !ctx.getSearchSource().sorts().isEmpty();
    }

    @Override
    protected void validate(ConversionContext ctx) throws ConversionException {
        ctx.requireRelNodeSupported(LogicalSort.class);
    }

    @Override
    protected RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException {
        RelDataType rowType = input.getRowType();
        List<RelFieldCollation> fieldCollations = new ArrayList<>();

        for (SortBuilder<?> sortBuilder : ctx.getSearchSource().sorts()) {
            if (sortBuilder instanceof FieldSortBuilder fieldSort) {
                String fieldName = fieldSort.getFieldName();
                RelDataTypeField field = rowType.getField(fieldName, true, false);
                if (field == null) {
                    throw ConversionException.invalidField(fieldName);
                }
                int fieldIndex = field.getIndex();

                RelFieldCollation.Direction direction = (fieldSort.order() == SortOrder.ASC)
                    ? RelFieldCollation.Direction.ASCENDING
                    : RelFieldCollation.Direction.DESCENDING;

                RelFieldCollation.NullDirection nullDirection = (fieldSort.order() == SortOrder.ASC)
                    ? RelFieldCollation.NullDirection.LAST
                    : RelFieldCollation.NullDirection.FIRST;

                fieldCollations.add(new RelFieldCollation(fieldIndex, direction, nullDirection));
            } else {
                throw new UnsupportedOperationException(
                    "Sort type not supported: " + sortBuilder.getClass().getSimpleName()
                );
            }
        }

        RelCollation collation = RelCollations.of(fieldCollations);
        return LogicalSort.create(input, collation, null, null);
    }
}
