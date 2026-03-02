/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.converter;

import org.apache.calcite.rel.core.AggregateCall;
import org.opensearch.calcite.exception.ConversionException;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;

/**
 * Visitor interface for converting OpenSearch metric aggregations to Calcite AggregateCall.
 *
 * Each visit method handles a specific metric aggregation type and returns the
 * corresponding Calcite AggregateCall. Bucket aggregations (terms, multi_terms)
 * are handled separately as GROUP BY fields in {@link AggregationInfo}.
 */
public interface AggregationBuilderVisitor {

    /**
     * Visits an AVG aggregation and converts it to a Calcite AggregateCall.
     *
     * @param aggregation the AVG aggregation builder
     * @return the Calcite AggregateCall representation
     * @throws ConversionException if the aggregation cannot be converted
     */
    AggregateCall visitAvgAggregation(AvgAggregationBuilder aggregation) throws ConversionException;

    /**
     * Visits a SUM aggregation and converts it to a Calcite AggregateCall.
     *
     * @param aggregation the SUM aggregation builder
     * @return the Calcite AggregateCall representation
     * @throws ConversionException if the aggregation cannot be converted
     */
    AggregateCall visitSumAggregation(SumAggregationBuilder aggregation) throws ConversionException;

    /**
     * Visits a MIN aggregation and converts it to a Calcite AggregateCall.
     *
     * @param aggregation the MIN aggregation builder
     * @return the Calcite AggregateCall representation
     * @throws ConversionException if the aggregation cannot be converted
     */
    AggregateCall visitMinAggregation(MinAggregationBuilder aggregation) throws ConversionException;

    /**
     * Visits a MAX aggregation and converts it to a Calcite AggregateCall.
     *
     * @param aggregation the MAX aggregation builder
     * @return the Calcite AggregateCall representation
     * @throws ConversionException if the aggregation cannot be converted
     */
    AggregateCall visitMaxAggregation(MaxAggregationBuilder aggregation) throws ConversionException;
}
