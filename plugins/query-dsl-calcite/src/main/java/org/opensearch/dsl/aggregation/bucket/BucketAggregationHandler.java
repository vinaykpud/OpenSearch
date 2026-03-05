/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.aggregation.AggregationConversionContext;
import org.opensearch.dsl.aggregation.AggregationHandler;
import org.opensearch.dsl.aggregation.AggregationMetadataBuilder;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;

import java.util.Collection;

/**
 * Strategy interface for bucket aggregation handlers (terms, multi_terms, etc.).
 *
 * Each implementation provides grouping info, bucket orders, and sub-aggregations
 * for one bucket aggregation type. The default {@link #contribute} pushes grouping
 * and order data to the builder and requests an implicit {@code _count}.
 */
public interface BucketAggregationHandler<T extends AggregationBuilder> extends AggregationHandler<T> {

    /**
     * Returns the grouping contribution for this bucket aggregation.
     *
     * @param agg the bucket aggregation builder
     * @return the grouping info
     */
    GroupingInfo getGrouping(T agg);

    /**
     * Returns the bucket order for post-aggregation sorting.
     *
     * @param agg the bucket aggregation builder
     * @return the bucket order
     */
    BucketOrder getOrder(T agg);

    /**
     * Returns sub-aggregations to recurse into.
     *
     * @param agg the bucket aggregation builder
     * @return the sub-aggregations
     */
    @Override
    Collection<AggregationBuilder> getSubAggregations(T agg);

    @Override
    default void contribute(T agg, AggregationMetadataBuilder builder, AggregationConversionContext ctx) {
        builder.addGrouping(getGrouping(agg));
        builder.addBucketOrder(getOrder(agg));
        builder.requestImplicitCount();
    }
}
