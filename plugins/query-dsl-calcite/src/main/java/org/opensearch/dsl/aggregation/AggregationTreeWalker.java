/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.dsl.aggregation.bucket.BucketShape;
import org.opensearch.dsl.aggregation.metric.MetricTranslator;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Recursive tree walker that detects distinct granularity levels in the
 * aggregation tree and produces one {@link AggregationMetadata} per level.
 *
 * A "granularity" is a unique GROUP BY key set determined by the accumulated
 * bucket nesting path. Metrics at different nesting depths produce separate
 * metadata instances, each yielding its own {@code LogicalAggregate}.
 */
public class AggregationTreeWalker {

    private final AggregationRegistry registry;

    /**
     * Creates a tree walker with the given type registry.
     *
     * @param registry the registry of aggregation types
     */
    public AggregationTreeWalker(AggregationRegistry registry) {
        this.registry = registry;
    }

    /**
     * Walks the aggregation tree and produces one AggregationMetadata per
     * distinct granularity level that contains metrics.
     *
     * @param aggs The top-level aggregation builders
     * @param ctx The aggregation conversion context
     * @param inputRowType The schema before aggregation
     * @return A list of AggregationMetadata, one per granularity
     * @throws ConversionException if any aggregation fails to convert
     */
    public List<AggregationMetadata> walk(Collection<AggregationBuilder> aggs, AggregationConversionContext ctx,
            RelDataType inputRowType) throws ConversionException {
        Map<String, AggregationMetadataBuilder> granularities = new LinkedHashMap<>();
        walkRecursive(aggs, new ArrayList<>(), granularities, ctx);

        List<AggregationMetadata> result = new ArrayList<>();
        for (AggregationMetadataBuilder builder : granularities.values()) {
            result.add(builder.build(inputRowType, ctx));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private void walkRecursive(
            Collection<AggregationBuilder> aggs,
            List<GroupingInfo> currentGroupings,
            Map<String, AggregationMetadataBuilder> granularities,
            AggregationConversionContext ctx) throws ConversionException {
        if (aggs == null || aggs.isEmpty()) {
            return;
        }

        for (AggregationBuilder agg : aggs) {
            AggregationType<AggregationBuilder> type = registry.findHandler(agg);

            if (type instanceof BucketShape) {
                handleBucket((BucketShape<AggregationBuilder>) type,
                    agg, currentGroupings, granularities, ctx);
            } else if (type instanceof MetricTranslator) {
                handleMetric((MetricTranslator<AggregationBuilder>) type,
                    agg, currentGroupings, granularities, ctx);
            }
        }
    }

    private void handleBucket(
            BucketShape<AggregationBuilder> shape,
            AggregationBuilder agg,
            List<GroupingInfo> currentGroupings,
            Map<String, AggregationMetadataBuilder> granularities,
            AggregationConversionContext ctx) throws ConversionException {

        List<GroupingInfo> accumulatedGroupings = new ArrayList<>(currentGroupings);
        accumulatedGroupings.add(shape.getGrouping(agg));

        // Create the builder for this granularity eagerly, with only this bucket's
        // own order. This ensures the order is set before metrics at this level
        // are processed, and avoids accumulating parent bucket orders which don't
        // apply at deeper granularities.
        List<BucketOrder> ownOrders = new ArrayList<>();
        BucketOrder order = shape.getOrder(agg);
        if (order != null) {
            ownOrders.add(order);
        }
        getOrCreateBuilder(accumulatedGroupings, ownOrders, granularities);

        walkRecursive(shape.getSubAggregations(agg), accumulatedGroupings, granularities, ctx);
    }

    private void handleMetric(
            MetricTranslator<AggregationBuilder> translator,
            AggregationBuilder agg,
            List<GroupingInfo> currentGroupings,
            Map<String, AggregationMetadataBuilder> granularities,
            AggregationConversionContext ctx) throws ConversionException {

        // Metrics at the top level (no bucket parent) have no groupings.
        // The builder is created with empty orders since there's no bucket to order by.
        AggregationMetadataBuilder builder = getOrCreateBuilder(
            currentGroupings, List.of(), granularities);
        builder.addAggregateCall(translator.toAggregateCall(agg, ctx));
        builder.addAggregateFieldName(translator.getAggregateFieldName(agg));
    }

    private AggregationMetadataBuilder getOrCreateBuilder(
            List<GroupingInfo> groupings,
            List<BucketOrder> orders,
            Map<String, AggregationMetadataBuilder> granularities) {
        String key = granularityKey(groupings);
        return granularities.computeIfAbsent(key, k -> {
            AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
            for (GroupingInfo g : groupings) {
                builder.addGrouping(g);
            }
            if (!groupings.isEmpty()) {
                builder.requestImplicitCount();
            }
            for (BucketOrder o : orders) {
                builder.addBucketOrder(o);
            }
            return builder;
        });
    }

    private static String granularityKey(List<GroupingInfo> groupings) {
        if (groupings.isEmpty()) {
            return "";
        }
        return groupings.stream()
            .flatMap(g -> g.getFieldNames().stream())
            .collect(Collectors.joining(","));
    }
}
