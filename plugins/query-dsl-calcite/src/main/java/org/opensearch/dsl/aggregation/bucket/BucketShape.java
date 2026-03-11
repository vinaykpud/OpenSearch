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
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;

import java.util.Collection;
import java.util.List;

/**
 * Shape interface for bucket aggregations (terms, multi_terms, etc.).
 *
 * Provides structural information for building Calcite plans and
 * response conversion back to OpenSearch InternalAggregation.
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

    /**
     * Constructs an InternalAggregation from grouped bucket entries.
     *
     * @param agg     the original aggregation builder (for name, order, etc.)
     * @param buckets the bucket entries with keys, doc counts, and sub-aggs
     * @return the constructed bucket InternalAggregation
     */
    InternalAggregation toBucketAggregation(T agg, List<BucketEntry> buckets);
}
