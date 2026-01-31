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
 * This visitor pattern allows different metric aggregation types to be converted to their
 * corresponding Calcite representations. Each visit method handles a specific
 * OpenSearch aggregation type and returns a Calcite AggregateCall.
 *
 * Note: Bucket aggregations (like terms) are handled separately via GROUP BY and do not
 * produce AggregateCall objects.
 *
 * Supported metric aggregations:
 * - AvgAggregationBuilder: Converts to AVG aggregate function
 * - SumAggregationBuilder: Converts to SUM aggregate function
 * - MinAggregationBuilder: Converts to MIN aggregate function
 * - MaxAggregationBuilder: Converts to MAX aggregate function
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
