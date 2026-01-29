/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.converter;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Analyzes sort specifications to determine if they apply before or after aggregation.
 * 
 * This class is responsible for:
 * 1. Classifying sorts as pre-aggregation or post-aggregation
 * 2. Extracting sort specifications from aggregation order parameters
 * 3. Handling special fields like _key and _count
 */
public class SortAnalyzer {

    /**
     * Creates a new SortAnalyzer instance.
     */
    public SortAnalyzer() {
        // Default constructor
    }

    /**
     * Analyzes sort specifications and determines if they apply before or after aggregation.
     * 
     * @param sorts List of SortBuilder from SearchSourceBuilder
     * @param aggregations Aggregations from SearchSourceBuilder
     * @param originalSchema Schema before aggregation
     * @return SortClassification with pre-agg and post-agg sorts separated
     */
    public SortClassification analyzeSorts(
        List<SortBuilder<?>> sorts,
        AggregatorFactories.Builder aggregations,
        RelDataType originalSchema
    ) {
        if (sorts == null || sorts.isEmpty()) {
            return new SortClassification(Collections.emptyList(), Collections.emptyList());
        }

        if (aggregations == null || aggregations.count() == 0) {
            // No aggregations - all sorts are pre-aggregation
            return new SortClassification(sorts, Collections.emptyList());
        }
        
        List<SortBuilder<?>> preAggSorts = new ArrayList<>();
        List<SortBuilder<?>> postAggSorts = new ArrayList<>();
        
        // Extract aggregation names
        Set<String> aggNames = extractAggregationNames(aggregations);
        Set<String> originalFieldNames = extractFieldNames(originalSchema);
        
        for (SortBuilder<?> sort : sorts) {
            String sortField = extractSortFieldName(sort);
            
            if (aggNames.contains(sortField)) {
                // Sort field matches aggregation name - post-aggregation sort
                postAggSorts.add(sort);
            } else if (originalFieldNames.contains(sortField)) {
                // Sort field exists in original schema - pre-aggregation sort
                preAggSorts.add(sort);
            } else {
                // Unknown field - could be error or special field like _score
                // For now, treat as pre-aggregation
                preAggSorts.add(sort);
            }
        }
        
        return new SortClassification(preAggSorts, postAggSorts);
    }
    
    /**
     * Extracts sort specifications from aggregation order parameters.
     * 
     * @param aggregations Aggregations from SearchSourceBuilder
     * @return List of sort specifications from aggregation order parameters
     */
    public List<AggregationOrderSort> extractAggregationOrderSorts(
        AggregatorFactories.Builder aggregations
    ) {
        if (aggregations == null || aggregations.count() == 0) {
            return Collections.emptyList();
        }

        List<AggregationOrderSort> orderSorts = new ArrayList<>();
        
        for (AggregationBuilder agg : aggregations.getAggregatorFactories()) {
            if (agg instanceof TermsAggregationBuilder) {
                TermsAggregationBuilder terms = (TermsAggregationBuilder) agg;
                BucketOrder order = terms.order();
                
                if (order != null) {
                    // Handle compound orders (multiple sort criteria)
                    extractOrderSortsFromBucketOrder(agg.getName(), order, orderSorts);
                }
            }
            // Note: Histogram and DateHistogram aggregations are not yet supported
        }
        
        return orderSorts;
    }

    /**
     * Extracts order sorts from a BucketOrder, handling both simple and compound orders.
     * 
     * @param aggregationName Name of the aggregation
     * @param order BucketOrder to extract from
     * @param orderSorts List to add extracted sorts to
     */
    private void extractOrderSortsFromBucketOrder(
        String aggregationName,
        BucketOrder order,
        List<AggregationOrderSort> orderSorts
    ) {
        // Check if it's a compound order (multiple sort criteria)
        if (order instanceof InternalOrder.CompoundOrder) {
            InternalOrder.CompoundOrder compound = (InternalOrder.CompoundOrder) order;
            List<BucketOrder> elements = compound.orderElements();
            
            // Extract all order elements
            for (BucketOrder element : elements) {
                String orderField = extractOrderField(element);
                boolean ascending = extractOrderDirection(element);
                
                orderSorts.add(new AggregationOrderSort(
                    aggregationName,
                    orderField,
                    ascending
                ));
            }
        } else {
            // Simple order - single sort criterion
            String orderField = extractOrderField(order);
            boolean ascending = extractOrderDirection(order);
            
            orderSorts.add(new AggregationOrderSort(
                aggregationName,
                orderField,
                ascending
            ));
        }
    }

    /**
     * Extracts aggregation names from aggregation builders.
     */
    private Set<String> extractAggregationNames(AggregatorFactories.Builder aggregations) {
        Set<String> names = new HashSet<>();
        for (AggregationBuilder agg : aggregations.getAggregatorFactories()) {
            names.add(agg.getName());
        }
        return names;
    }

    /**
     * Extracts field names from schema.
     */
    private Set<String> extractFieldNames(RelDataType schema) {
        Set<String> names = new HashSet<>();
        for (RelDataTypeField field : schema.getFieldList()) {
            names.add(field.getName());
        }
        return names;
    }

    /**
     * Extracts sort field name from SortBuilder.
     */
    private String extractSortFieldName(SortBuilder<?> sort) {
        if (sort instanceof FieldSortBuilder) {
            return ((FieldSortBuilder) sort).getFieldName();
        }
        // For other sort types, return empty string
        return "";
    }

    /**
     * Extracts the field name from BucketOrder.
     * Handles special fields like _key and _count.
     * Note: This method should NOT be called directly on CompoundOrder - 
     * use extractOrderSortsFromBucketOrder() instead.
     */
    private String extractOrderField(BucketOrder order) {
        // Check if it's an Aggregation order (sorting by sub-aggregation result)
        if (order instanceof InternalOrder.Aggregation) {
            InternalOrder.Aggregation aggOrder = (InternalOrder.Aggregation) order;
            return aggOrder.path().toString();
        }
        
        // Check if it's a key order
        if (InternalOrder.isKeyOrder(order)) {
            return "_key";
        }
        
        // Default to _count for all other cases (including COUNT_ASC and COUNT_DESC)
        return "_count";
    }

    /**
     * Extracts the sort direction from BucketOrder.
     * Note: This method should NOT be called directly on CompoundOrder - 
     * use extractOrderSortsFromBucketOrder() instead.
     */
    private boolean extractOrderDirection(BucketOrder order) {
        // Check if it's an Aggregation order
        if (order instanceof InternalOrder.Aggregation) {
            InternalOrder.Aggregation aggOrder = (InternalOrder.Aggregation) order;
            // The path() method doesn't expose direction, but we can check via toString
            // which outputs the SortOrder. For now, check if it's ascending via the order field
            String pathStr = aggOrder.path().toString();
            // This is a simplification - in practice, Aggregation orders store direction internally
            // We'll need to use reflection or toString parsing
            String orderStr = order.toString();
            return orderStr.toLowerCase().contains("asc");
        }
        
        // Check if it's key ascending
        if (InternalOrder.isKeyAsc(order)) {
            return true;
        }
        
        // Check if it's key descending
        if (InternalOrder.isKeyDesc(order)) {
            return false;
        }
        
        // Check if it's count descending (default for most aggregations)
        if (InternalOrder.isCountDesc(order)) {
            return false;
        }
        
        // Default to ascending for count ascending or unknown orders
        return true;
    }

    /**
     * Inner class to hold classification results.
     */
    public static class SortClassification {
        private final List<SortBuilder<?>> preAggSorts;
        private final List<SortBuilder<?>> postAggSorts;

        /**
         * Creates a new SortClassification.
         * 
         * @param preAggSorts Sorts to apply before aggregation
         * @param postAggSorts Sorts to apply after aggregation
         */
        public SortClassification(List<SortBuilder<?>> preAggSorts, List<SortBuilder<?>> postAggSorts) {
            this.preAggSorts = preAggSorts;
            this.postAggSorts = postAggSorts;
        }

        /**
         * Gets the sorts to apply before aggregation.
         * 
         * @return List of pre-aggregation sorts
         */
        public List<SortBuilder<?>> getPreAggSorts() {
            return preAggSorts;
        }

        /**
         * Gets the sorts to apply after aggregation.
         * 
         * @return List of post-aggregation sorts
         */
        public List<SortBuilder<?>> getPostAggSorts() {
            return postAggSorts;
        }
    }

    /**
     * Inner class to represent aggregation order sort specifications.
     */
    public static class AggregationOrderSort {
        private final String aggregationName;
        private final String field;
        private final boolean ascending;

        /**
         * Creates a new AggregationOrderSort.
         * 
         * @param aggregationName Name of the aggregation
         * @param field Field to sort by (_key, _count, or aggregate field name)
         * @param ascending True for ascending order, false for descending
         */
        public AggregationOrderSort(String aggregationName, String field, boolean ascending) {
            this.aggregationName = aggregationName;
            this.field = field;
            this.ascending = ascending;
        }

        /**
         * Gets the aggregation name.
         * 
         * @return Aggregation name
         */
        public String getAggregationName() {
            return aggregationName;
        }

        /**
         * Gets the sort field.
         * 
         * @return Sort field (_key, _count, or aggregate field name)
         */
        public String getField() {
            return field;
        }

        /**
         * Gets the sort direction.
         * 
         * @return True for ascending, false for descending
         */
        public boolean isAscending() {
            return ascending;
        }
    }
}
