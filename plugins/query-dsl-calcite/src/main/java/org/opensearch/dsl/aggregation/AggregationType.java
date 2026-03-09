/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.opensearch.search.aggregations.AggregationBuilder;

/**
 * Base type interface for aggregation types.
 *
 * Provides only type identification for the {@link AggregationRegistry}.
 * Bucket and metric subtypes define their own contracts:
 * <ul>
 *   <li>{@link org.opensearch.dsl.aggregation.bucket.BucketShape} — structural
 *       (grouping, order, sub-aggregations)</li>
 *   <li>{@link org.opensearch.dsl.aggregation.metric.MetricTranslator} — data
 *       (aggregate calls contributed to a builder)</li>
 * </ul>
 */
public interface AggregationType<T extends AggregationBuilder> {

    /** Returns the concrete AggregationBuilder class this type processes. */
    Class<T> getAggregationType();
}
