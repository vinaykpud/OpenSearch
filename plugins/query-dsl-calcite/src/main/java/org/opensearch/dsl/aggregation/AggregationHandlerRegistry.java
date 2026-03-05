/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.dsl.pipeline.HandlerRegistry;
import org.opensearch.search.aggregations.AggregationBuilder;

/**
 * Registry of {@link AggregationHandler} strategies.
 *
 * Uses a map keyed by concrete AggregationBuilder class for O(1) lookup.
 */
public class AggregationHandlerRegistry extends HandlerRegistry<AggregationBuilder, AggregationHandler<?>> {

    public AggregationHandlerRegistry() {
        super("aggregation");
    }

    public void register(AggregationHandler<?> handler) {
        doRegister(handler.getAggregationType(), handler);
    }

    @SuppressWarnings("unchecked")
    public <T extends AggregationBuilder> AggregationHandler<T> findHandler(T agg) throws ConversionException {
        return (AggregationHandler<T>) super.findHandler(agg);
    }
}
