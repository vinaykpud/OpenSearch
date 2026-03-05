/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.search.aggregations.BucketOrder;

import java.util.List;

/**
 * Immutable metadata collected from walking the aggregation tree.
 *
 * Contains the resolved GROUP BY bit set, aggregate calls, bucket orders,
 * and field name lists needed by downstream converters to build
 * {@code LogicalAggregate} and {@code LogicalSort} nodes.
 *
 * Does NOT compute collations — that is the responsibility of
 * {@link org.opensearch.dsl.pipeline.CollationResolver}, which has access
 * to the actual post-aggregation schema.
 *
 * Constructed exclusively by {@link AggregationMetadataBuilder#build}.
 */
public final class AggregationMetadata {

    private final ImmutableBitSet groupByBitSet;
    private final List<String> groupByFieldNames;
    private final List<String> aggregateFieldNames;
    private final List<AggregateCall> aggregateCalls;
    private final List<BucketOrder> bucketOrders;

    AggregationMetadata(
        ImmutableBitSet groupByBitSet,
        List<String> groupByFieldNames,
        List<String> aggregateFieldNames,
        List<AggregateCall> aggregateCalls,
        List<BucketOrder> bucketOrders
    ) {
        this.groupByBitSet = groupByBitSet;
        this.groupByFieldNames = groupByFieldNames;
        this.aggregateFieldNames = aggregateFieldNames;
        this.aggregateCalls = aggregateCalls;
        this.bucketOrders = bucketOrders;
    }

    public ImmutableBitSet getGroupByBitSet() {
        return groupByBitSet;
    }

    public List<String> getGroupByFieldNames() {
        return groupByFieldNames;
    }

    public List<String> getAggregateFieldNames() {
        return aggregateFieldNames;
    }

    public List<AggregateCall> getAggregateCalls() {
        return aggregateCalls;
    }

    public List<BucketOrder> getBucketOrders() {
        return bucketOrders;
    }

    public boolean hasBucketOrders() {
        return !bucketOrders.isEmpty();
    }
}
