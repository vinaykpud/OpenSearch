/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.converter;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.calcite.exception.ConversionException;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides field index mapping for post-aggregation operations.
 *
 * This class is responsible for:
 * 1. Extracting GROUP BY fields from bucket aggregations
 * 2. Extracting aggregate field names from metric aggregations
 * 3. Building a field-to-index map for the post-aggregation schema
 * 4. Mapping field names to indices for post-aggregation sorting
 * 5. Handling special fields like _key and _count
 * 6. Providing ImmutableBitSet for GROUP BY fields (for LogicalAggregate)
 */
public class AggregationInfo {
    private final List<RelFieldCollation> collations;
    private final ImmutableBitSet groupByBitSet;

    private AggregationInfo(
        List<RelFieldCollation> collations,
        ImmutableBitSet groupByBitSet
    ) {
        this.collations = collations;
        this.groupByBitSet = groupByBitSet;
    }

    /**
     * Returns the ImmutableBitSet representing GROUP BY field indices.
     * Used when creating LogicalAggregate nodes.
     *
     * @return ImmutableBitSet of field indices to group by
     */
    public ImmutableBitSet getGroupByBitSet() {
        return groupByBitSet;
    }

    /**
     * Returns the pre-built collations for post-aggregation sorting.
     *
     * @return List of RelFieldCollation
     */
    public List<RelFieldCollation> getCollations() {
        return collations;
    }

    /**
     * Processes aggregations recursively, extracting GROUP BY fields, aggregate field names,
     * and bucket orders in a single tree walk.
     *
     * @param aggregations Collection of aggregation builders to process
     * @param groupByFields List to add GROUP BY field names to
     * @param aggregateFieldNames List to add aggregate field names to
     * @param bucketOrders List to add flattened bucket orders to
     * @throws ConversionException if an unknown aggregation type is encountered
     */
    private static void processAggregations(
        Collection<AggregationBuilder> aggregations,
        List<String> groupByFields,
        List<String> aggregateFieldNames,
        List<BucketOrder> bucketOrders
    ) throws ConversionException {
        if (aggregations == null || aggregations.isEmpty()) {
            return;
        }

        for (AggregationBuilder agg : aggregations) {
            if (agg instanceof TermsAggregationBuilder terms) {
                groupByFields.add(terms.field());
                flattenOrder(terms.order(), bucketOrders);
                processAggregations(terms.getSubAggregations(), groupByFields, aggregateFieldNames, bucketOrders);
            } else if (agg instanceof MultiTermsAggregationBuilder multiTerms) {
                for (MultiTermsValuesSourceConfig config : multiTerms.termsConfig()) {
                    groupByFields.add(config.getFieldName());
                }
                flattenOrder(multiTerms.order(), bucketOrders);
                processAggregations(multiTerms.getSubAggregations(), groupByFields, aggregateFieldNames, bucketOrders);
            } else if (agg instanceof AvgAggregationBuilder
                || agg instanceof SumAggregationBuilder
                || agg instanceof MinAggregationBuilder
                || agg instanceof MaxAggregationBuilder) {
                aggregateFieldNames.add(agg.getName());
            } else {
                throw new ConversionException(
                    "aggregation-processing",
                    "Unknown aggregation type: " + agg.getClass().getSimpleName() +
                    " (name: " + agg.getName() + "). "
                );
            }
        }
    }

    /**
     * Flattens a BucketOrder into individual order elements.
     * Handles both simple orders and compound orders.
     */
    private static void flattenOrder(BucketOrder order, List<BucketOrder> out) {
        if (order == null) return;
        if (order instanceof InternalOrder.CompoundOrder compound) {
            out.addAll(compound.orderElements());
        } else {
            out.add(order);
        }
    }

    /**
     * Builds a sort-field-to-indices map for post-aggregation sorting.
     * Post-agg schema order: [GROUP BY fields] + [aggregate fields] + [_count]
     *
     * Entries:
     * - _key → all GROUP BY field indices (supports multi_terms expansion)
     * - each aggregate field name → its single index
     * - _count → its single index
     *
     * GROUP BY fields themselves are not added since they are only sortable via _key.
     *
     * @param groupByFields List of GROUP BY field names
     * @param aggregateFieldNames List of aggregate field names
     * @return Map from sortable field name to list of indices in post-aggregation schema
     */
    private static Map<String, List<Integer>> buildSortFieldMap(
        List<String> groupByFields,
        List<String> aggregateFieldNames
    ) {
        Map<String, List<Integer>> sortFieldMap = new HashMap<>();
        int fieldIndex = 0;

        // _key maps to all GROUP BY field indices
        List<Integer> keyIndices = new ArrayList<>(groupByFields.size());
        for (int i = 0; i < groupByFields.size(); i++) {
            keyIndices.add(fieldIndex++);
        }
        sortFieldMap.put("_key", keyIndices);

        // Each aggregate field maps to its single index
        for (String aggName : aggregateFieldNames) {
            sortFieldMap.put(aggName, List.of(fieldIndex++));
        }

        // _count is always present
        sortFieldMap.put("_count", List.of(fieldIndex));

        return sortFieldMap;
    }

    /**
     * Builds an ImmutableBitSet representing GROUP BY field indices in the original schema.
     * Used when creating LogicalAggregate nodes.
     *
     * @param groupByFields List of GROUP BY field names
     * @param originalSchema The schema before aggregation
     * @return ImmutableBitSet of field indices to group by
     * @throws ConversionException if field lookup fails
     */
    private static ImmutableBitSet buildGroupByBitSet(
        List<String> groupByFields,
        RelDataType originalSchema
    ) throws ConversionException {
        List<Integer> groupByIndices = new ArrayList<>();
        for (String fieldName : groupByFields) {
            int index = SchemaUtils.findFieldIndex(fieldName, originalSchema);
            groupByIndices.add(index);
        }
        return ImmutableBitSet.of(groupByIndices);
    }

    /**
     * Builds aggregation metadata from aggregation builders.
     *
     * @param aggregations Aggregations from SearchSourceBuilder
     * @param originalSchema The schema before aggregation (for converting field names to indices)
     * @return AggregationInfo with computed GROUP BY bit set
     * @throws ConversionException if field lookup fails
     */
    public static AggregationInfo build(
        AggregatorFactories.Builder aggregations,
        RelDataType originalSchema
    ) throws ConversionException {
        List<String> groupByFields = new ArrayList<>();
        List<String> aggregateFieldNames = new ArrayList<>();
        List<BucketOrder> bucketOrders = new ArrayList<>();

        // Process all aggregations (top-level and nested) — single tree walk
        processAggregations(aggregations.getAggregatorFactories(), groupByFields, aggregateFieldNames, bucketOrders);

        // Build sort field map
        Map<String, List<Integer>> sortFieldMap = buildSortFieldMap(groupByFields, aggregateFieldNames);

        // Build group by bit set
        ImmutableBitSet groupByBitSet = buildGroupByBitSet(groupByFields, originalSchema);

        // Build collations from bucket orders
        List<RelFieldCollation> collations = buildCollations(bucketOrders, sortFieldMap);

        return new AggregationInfo(Collections.unmodifiableList(collations), groupByBitSet);
    }

    /**
     * Builds collations from bucket orders using the sort field map.
     */
    private static List<RelFieldCollation> buildCollations(
        List<BucketOrder> bucketOrders,
        Map<String, List<Integer>> sortFieldMap
    ) throws ConversionException {
        List<RelFieldCollation> collations = new ArrayList<>();
        for (BucketOrder bucketOrder : bucketOrders) {
            OrderMetadata metadata = OrderMetadata.fromBucketOrder(bucketOrder);
            List<Integer> indices = sortFieldMap.get(metadata.getFieldName());
            if (indices == null) {
                throw new ConversionException("post-aggregation-sort", "Sort field '" + metadata.getFieldName() + "' not found. Available: " + sortFieldMap.keySet());
            }
            for (int fieldIndex : indices) {
                collations.add(createCollation(fieldIndex, metadata.isAscending()));
            }
        }
        return collations;
    }

    /**
     * Creates a RelFieldCollation with consistent null direction handling.
     */
    private static RelFieldCollation createCollation(int fieldIndex, boolean ascending) {
        RelFieldCollation.Direction direction = ascending
            ? RelFieldCollation.Direction.ASCENDING
            : RelFieldCollation.Direction.DESCENDING;
        RelFieldCollation.NullDirection nullDirection = ascending
            ? RelFieldCollation.NullDirection.LAST
            : RelFieldCollation.NullDirection.FIRST;
        return new RelFieldCollation(fieldIndex, direction, nullDirection);
    }
}
