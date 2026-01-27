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
 */
public interface AggregationBuilderVisitor {

    /**
     * Visits an AvgAggregationBuilder and converts it to a Calcite AggregateCall.
     *
     * The avg aggregation computes the average value of a numeric field.
     * This is converted to Calcite's AVG aggregate function.
     *
     * @param aggregation The OpenSearch avg aggregation
     * @return Calcite AggregateCall representing the AVG function
     * @throws ConversionException if conversion fails
     */
    AggregateCall visitAvgAggregation(AvgAggregationBuilder aggregation) throws ConversionException;
}
