/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.rel.core.AggregateCall;
import org.opensearch.dsl.aggregation.AggregationConversionContext;
import org.opensearch.dsl.aggregation.AggregationHandler;
import org.opensearch.dsl.aggregation.AggregationMetadataBuilder;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.search.aggregations.AggregationBuilder;

/**
 * Strategy interface for metric aggregation handlers (AVG, SUM, MIN, MAX, etc.).
 *
 * Each implementation converts one metric aggregation type to a Calcite AggregateCall.
 * The default {@link #contribute} pushes the aggregate call and field name to the builder.
 */
public interface MetricAggregationHandler<T extends AggregationBuilder> extends AggregationHandler<T> {

    /**
     * Converts the metric aggregation to a Calcite AggregateCall.
     *
     * @param agg The metric aggregation builder
     * @param ctx The aggregation conversion context
     * @return The Calcite AggregateCall
     * @throws ConversionException if the conversion fails
     */
    AggregateCall toAggregateCall(T agg, AggregationConversionContext ctx) throws ConversionException;

    /**
     * Returns the name of the aggregate field (the aggregation name, used for post-agg schema).
     *
     * @param agg The metric aggregation builder
     * @return The aggregate field name
     */
    String getAggregateFieldName(T agg);

    @Override
    default void contribute(T agg, AggregationMetadataBuilder builder,
            AggregationConversionContext ctx) throws ConversionException {
        builder.addAggregateCall(toAggregateCall(agg, ctx));
        builder.addAggregateFieldName(getAggregateFieldName(agg));
    }
}
