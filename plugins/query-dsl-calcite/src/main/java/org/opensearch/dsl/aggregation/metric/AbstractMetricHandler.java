/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlAggFunction;
import org.opensearch.dsl.aggregation.AggregationConversionContext;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.search.aggregations.AggregationBuilder;

import java.util.Collections;

/**
 * Base class for metric aggregation handlers. Provides the common
 * {@link #toAggregateCall} logic, leaving subclasses to supply the
 * SQL aggregate function and field name extraction.
 */
public abstract class AbstractMetricHandler<T extends AggregationBuilder> implements MetricAggregationHandler<T> {

    /** Creates a new abstract metric handler. */
    protected AbstractMetricHandler() {}

    /** Returns the SQL aggregate function for this metric (e.g., AVG, SUM). */
    protected abstract SqlAggFunction getAggFunction();

    /**
     * Returns the field name from the aggregation builder.
     *
     * @param agg the aggregation builder
     * @return the field name
     */
    protected abstract String getFieldName(T agg);

    @Override
    public AggregateCall toAggregateCall(T agg, AggregationConversionContext ctx)
            throws ConversionException {
        ctx.requireAggFunctionSupported(getAggFunction());

        String fieldName = getFieldName(agg);
        RelDataTypeField field = ctx.getRowType().getField(fieldName, true, false);
        if (field == null) {
            throw ConversionException.invalidField(fieldName);
        }
        int fieldIndex = field.getIndex();
        RelDataType fieldType = field.getType();

        return AggregateCall.create(
            getAggFunction(),
            false,
            false,
            false,
            Collections.singletonList(fieldIndex),
            -1,
            RelCollations.EMPTY,
            fieldType,
            agg.getName()
        );
    }

    @Override
    public String getAggregateFieldName(T agg) {
        return agg.getName();
    }
}
