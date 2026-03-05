/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.search.aggregations.AggregationBuilder;

import java.util.Collection;
import java.util.Collections;

/**
 * Base strategy interface for aggregation handlers.
 *
 * Each handler knows how to contribute its aggregation metadata to the
 * {@link AggregationMetadataBuilder}. The walker calls {@link #contribute}
 * and {@link #getSubAggregations} — no instanceof dispatch needed.
 */
public interface AggregationHandler<T extends AggregationBuilder> {

    /** Returns the concrete AggregationBuilder class this handler processes. */
    Class<T> getAggregationType();

    /**
     * Contributes this aggregation's metadata to the builder.
     *
     * @param agg The aggregation builder
     * @param builder The mutable result builder
     * @param ctx The aggregation conversion context
     * @throws ConversionException if conversion fails
     */
    void contribute(T agg, AggregationMetadataBuilder builder, AggregationConversionContext ctx)
        throws ConversionException;

    /**
     * Returns sub-aggregations to recurse into.
     * Default: empty (metric handlers have no children).
     *
     * @param agg the aggregation builder
     * @return the sub-aggregations, or empty if none
     */
    default Collection<AggregationBuilder> getSubAggregations(T agg) {
        return Collections.emptyList();
    }
}
