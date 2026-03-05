/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.search.aggregations.AggregationBuilder;

import java.util.Collection;

/**
 * Unified recursive tree walker that collects GROUP BY fields, bucket orders,
 * and AggregateCall objects in a single pass via handler contributions.
 *
 * Each handler's {@link AggregationHandler#contribute} method pushes its data
 * to the builder — no instanceof dispatch needed in the walker.
 */
public class AggregationTreeWalker {

    private final AggregationHandlerRegistry registry;

    /**
     * Creates a tree walker with the given handler registry.
     *
     * @param registry the registry of aggregation handlers
     */
    public AggregationTreeWalker(AggregationHandlerRegistry registry) {
        this.registry = registry;
    }

    /**
     * Walks the aggregation tree and produces an immutable AggregationMetadata.
     *
     * @param aggs The top-level aggregation builders
     * @param ctx The aggregation conversion context
     * @param inputRowType The schema before aggregation
     * @return The aggregation result
     * @throws ConversionException if any aggregation fails to convert
     */
    public AggregationMetadata walk(Collection<AggregationBuilder> aggs, AggregationConversionContext ctx,
            RelDataType inputRowType) throws ConversionException {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        walkRecursive(aggs, builder, ctx);
        return builder.build(inputRowType, ctx);
    }

    private void walkRecursive(Collection<AggregationBuilder> aggs, AggregationMetadataBuilder builder,
            AggregationConversionContext ctx) throws ConversionException {
        if (aggs == null || aggs.isEmpty()) {
            return;
        }

        for (AggregationBuilder agg : aggs) {
            AggregationHandler<AggregationBuilder> handler = registry.findHandler(agg);
            handler.contribute(agg, builder, ctx);
            walkRecursive(handler.getSubAggregations(agg), builder, ctx);
        }
    }
}
