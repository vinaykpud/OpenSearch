/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.converter;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.calcite.exception.ConversionException;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;

import java.util.ArrayList;
import java.util.Collection;
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
    private final List<String> groupByFields;
    private final Map<String, Integer> fieldIndexMap;
    private final ImmutableBitSet groupByBitSet;

    /**
     * Enum for classifying aggregation types.
     */
    private enum AggregationType {
        BUCKET,
        METRIC
    }

    /**
     * Classifies an aggregation builder by type.
     *
     * @param agg The aggregation builder to classify
     * @return The aggregation type, or null if unknown
     */
    private static AggregationType classifyAggregation(AggregationBuilder agg) {
        if (agg instanceof TermsAggregationBuilder) {
            return AggregationType.BUCKET;
        }
        if (agg instanceof AvgAggregationBuilder ||
            agg instanceof SumAggregationBuilder ||
            agg instanceof MinAggregationBuilder ||
            agg instanceof MaxAggregationBuilder) {
            return AggregationType.METRIC;
        }
        return null;
    }

    private AggregationInfo(
        List<String> groupByFields,
        Map<String, Integer> fieldIndexMap,
        ImmutableBitSet groupByBitSet
    ) {
        this.groupByFields = groupByFields;
        this.fieldIndexMap = fieldIndexMap;
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
     * Processes aggregations recursively, extracting GROUP BY fields and aggregate field names.
     * Handles both top-level and nested aggregations.
     *
     * @param aggregations Collection of aggregation builders to process
     * @param groupByFields List to add GROUP BY field names to
     * @param aggregateFieldNames List to add aggregate field names to
     * @throws ConversionException if an unknown aggregation type is encountered
     */
    private static void processAggregations(
        Collection<AggregationBuilder> aggregations,
        List<String> groupByFields,
        List<String> aggregateFieldNames
    ) throws ConversionException {
        if (aggregations == null || aggregations.isEmpty()) {
            return;
        }

        for (AggregationBuilder agg : aggregations) {
            AggregationType type = classifyAggregation(agg);

            if (type == AggregationType.BUCKET) {
                TermsAggregationBuilder terms = (TermsAggregationBuilder) agg;
                groupByFields.add(terms.field());
                processAggregations(terms.getSubAggregations(), groupByFields, aggregateFieldNames);
            } else if (type == AggregationType.METRIC) {
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
     * Builds a field-to-index map for the post-aggregation schema.
     * Post-agg schema order: [GROUP BY fields] + [aggregate fields] + [_count]
     *
     * @param groupByFields List of GROUP BY field names
     * @param aggregateFieldNames List of aggregate field names
     * @return Map from field name to index in post-aggregation schema
     */
    private static Map<String, Integer> buildFieldIndexMap(
        List<String> groupByFields,
        List<String> aggregateFieldNames
    ) {
        Map<String, Integer> fieldIndexMap = new HashMap<>();
        int fieldIndex = 0;

        // Add GROUP BY fields
        for (String field : groupByFields) {
            fieldIndexMap.put(field, fieldIndex++);
        }

        // Add aggregate fields
        for (String aggName : aggregateFieldNames) {
            fieldIndexMap.put(aggName, fieldIndex++);
        }

        // Add implicit _count field (always present in aggregations)
        fieldIndexMap.put("_count", fieldIndex);

        return fieldIndexMap;
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

        // Process all aggregations (top-level and nested)
        processAggregations(aggregations.getAggregatorFactories(), groupByFields, aggregateFieldNames);

        // Build field-to-index map
        Map<String, Integer> fieldIndexMap = buildFieldIndexMap(groupByFields, aggregateFieldNames);

        // Build group by bit set
        ImmutableBitSet groupByBitSet = buildGroupByBitSet(groupByFields, originalSchema);

        return new AggregationInfo(groupByFields, fieldIndexMap, groupByBitSet);
    }

    /**
     * Maps a sort field name to its index in the post-aggregation schema.
     * Handles special fields like _key and _count.
     *
     * @param sortField The field name to map
     * @return The index of the field in the post-aggregation schema
     * @throws ConversionException if the field is not found
     */
    public int mapSortFieldToIndex(String sortField) throws ConversionException {
        // Handle special _key field (maps to first GROUP BY field)
        if ("_key".equals(sortField)) {
            return mapKeyField();
        }

        // Handle regular fields (includes _count)
        Integer index = fieldIndexMap.get(sortField);
        if (index != null) {
            return index;
        }

        // Field not found
        throw buildFieldNotFoundException(sortField);
    }

    /**
     * Maps the special _key field to the first GROUP BY field index.
     *
     * @return The index of the first GROUP BY field
     * @throws ConversionException if there are no GROUP BY fields
     */
    private int mapKeyField() throws ConversionException {
        if (groupByFields.isEmpty()) {
            throw new ConversionException(
                "post-aggregation-sort",
                "Cannot sort by _key without GROUP BY fields. " +
                "The query must include a bucket aggregation (terms, histogram, date_histogram) to use _key sorting."
            );
        }
        return fieldIndexMap.get(groupByFields.get(0));
    }

    /**
     * Builds a ConversionException for when a sort field is not found.
     *
     * @param sortField The field name that was not found
     * @return ConversionException with detailed error message
     */
    private ConversionException buildFieldNotFoundException(String sortField) {
        return new ConversionException(
            "post-aggregation-sort",
            "Sort field '" + sortField + "' not found in post-aggregation schema. " +
            "Available fields: " + String.join(", ", fieldIndexMap.keySet()) + ". " +
            "Post-aggregation sorts can only reference GROUP BY fields, aggregate result fields, or special fields (_key, _count)."
        );
    }
}
