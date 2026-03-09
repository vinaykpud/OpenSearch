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
 * Registry of {@link AggregationType} implementations.
 *
 * Uses a map keyed by concrete AggregationBuilder class for O(1) lookup.
 */
public class AggregationRegistry extends HandlerRegistry<AggregationBuilder, AggregationType<?>> {

    /** Creates a new aggregation type registry. */
    public AggregationRegistry() {
        super("aggregation");
    }

    /**
     * Registers an aggregation type.
     *
     * @param handler the aggregation type to register
     */
    public void register(AggregationType<?> handler) {
        doRegister(handler.getAggregationType(), handler);
    }

    /**
     * Finds the aggregation type for the given aggregation builder.
     *
     * @param agg the aggregation builder
     * @param <T> the aggregation builder type
     * @return the matching aggregation type
     * @throws ConversionException if no aggregation type is registered
     */
    @SuppressWarnings("unchecked")
    public <T extends AggregationBuilder> AggregationType<T> findHandler(T agg) throws ConversionException {
        return (AggregationType<T>) super.findHandler(agg);
    }
}
