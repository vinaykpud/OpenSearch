/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.aggregation.AggregationType;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;

import java.util.Collection;

/**
 * Shape interface for bucket aggregations (terms, multi_terms, etc.).
 *
 * Provides structural information that the
 * {@link org.opensearch.dsl.aggregation.AggregationTreeWalker} uses to build the
 * multi-granularity tree: grouping fields, bucket order, and sub-aggregations.
 */
public interface BucketShape<T extends AggregationBuilder> extends AggregationType<T> {

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
    Collection<AggregationBuilder> getSubAggregations(T agg);
}
