/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.converter;

import org.apache.calcite.rel.type.RelDataType;
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
 */
public class AggregationInfo {
    private final List<String> groupByFields;
    private final Map<String, Integer> fieldIndexMap;

    private AggregationInfo(
        List<String> groupByFields,
        Map<String, Integer> fieldIndexMap
    ) {
        this.groupByFields = groupByFields;
        this.fieldIndexMap = fieldIndexMap;
    }

    /**
     * Builds aggregation metadata from aggregation builders.
     *
     * @param aggregations Aggregations from SearchSourceBuilder
     * @return AggregationInfo with field-to-index mappings
     */
    public static AggregationInfo build(
        AggregatorFactories.Builder aggregations
    ) {
        List<String> groupByFields = new ArrayList<>();
        List<String> aggregateFieldNames = new ArrayList<>();

        // Extract GROUP BY fields from bucket aggregations and their sub-aggregations
        for (AggregationBuilder agg : aggregations.getAggregatorFactories()) {
            if (agg instanceof TermsAggregationBuilder terms) {
                groupByFields.add(terms.field());
                extractSubAggregations(terms.getSubAggregations(), aggregateFieldNames);
            }
        }

        // Extract aggregate field names from top-level metric aggregations
        for (AggregationBuilder agg : aggregations.getAggregatorFactories()) {
            if (agg instanceof AvgAggregationBuilder ||
                agg instanceof SumAggregationBuilder ||
                agg instanceof MinAggregationBuilder ||
                agg instanceof MaxAggregationBuilder) {
                aggregateFieldNames.add(agg.getName());
            }
        }

        // Build field-to-index map
        // Post-agg schema order: [GROUP BY fields] + [aggregate fields] + [_count]
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

        return new AggregationInfo(groupByFields, fieldIndexMap);
    }

    /**
     * Recursively extracts metric aggregations from sub-aggregations.
     *
     * @param subAggregations Sub-aggregations to extract from
     * @param aggregateFields List to add aggregate field names to
     */
    private static void extractSubAggregations(
        Collection<AggregationBuilder> subAggregations,
        List<String> aggregateFields
    ) {
        if (subAggregations == null || subAggregations.isEmpty()) {
            return;
        }

        for (AggregationBuilder subAgg : subAggregations) {
            if (subAgg instanceof AvgAggregationBuilder ||
                subAgg instanceof SumAggregationBuilder ||
                subAgg instanceof MinAggregationBuilder ||
                subAgg instanceof MaxAggregationBuilder) {
                aggregateFields.add(subAgg.getName());
            }

            // Recursively handle nested bucket aggregations
            if (subAgg instanceof TermsAggregationBuilder) {
                TermsAggregationBuilder terms = (TermsAggregationBuilder) subAgg;
                extractSubAggregations(terms.getSubAggregations(), aggregateFields);
            }
        }
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
            if (groupByFields.isEmpty()) {
                throw new ConversionException(
                    "post-aggregation-sort",
                    "Cannot sort by _key without GROUP BY fields. " +
                    "The query must include a bucket aggregation (terms, histogram, date_histogram) to use _key sorting."
                );
            }
            return fieldIndexMap.get(groupByFields.get(0));
        }

        // Handle _count field
        if ("_count".equals(sortField)) {
            return fieldIndexMap.get("_count");
        }

        // Handle regular field names
        Integer index = fieldIndexMap.get(sortField);
        if (index == null) {
            throw new ConversionException(
                "post-aggregation-sort",
                "Sort field '" + sortField + "' not found in post-aggregation schema. " +
                "Available fields: " + String.join(", ", fieldIndexMap.keySet()) + ". " +
                "Post-aggregation sorts can only reference GROUP BY fields, aggregate result fields, or special fields (_key, _count)."
            );
        }
        return index;
    }
}
