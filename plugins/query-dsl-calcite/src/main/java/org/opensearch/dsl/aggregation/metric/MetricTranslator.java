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
import org.opensearch.dsl.aggregation.AggregationType;
import org.opensearch.dsl.aggregation.AggregationMetadataBuilder;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.search.aggregations.AggregationBuilder;

/**
 * Translator interface for metric aggregations (AVG, SUM, MIN, MAX, etc.).
 *
 * Each implementation converts one metric aggregation type to a Calcite AggregateCall.
 */
public interface MetricTranslator<T extends AggregationBuilder> extends AggregationType<T> {

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
}
