/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.search.aggregations.InternalAggregations;

import java.util.List;

/**
 * Represents a single bucket entry for response construction.
 *
 * @param keys     the bucket key values (single element for terms, multiple for multi_terms)
 * @param docCount the document count for this bucket
 * @param subAggs  the sub-aggregation results for this bucket
 */
public record BucketEntry(List<Object> keys, long docCount, InternalAggregations subAggs) {}
