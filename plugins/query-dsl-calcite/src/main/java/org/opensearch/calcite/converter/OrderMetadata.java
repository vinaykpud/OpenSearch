/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.converter;

import java.util.Objects;

/**
 * Immutable value object containing metadata extracted from a BucketOrder.
 * 
 * This class holds all information needed to create a Calcite RelFieldCollation
 * from an OpenSearch BucketOrder, including the field name to sort by, the sort
 * direction, and the type of order.
 */
public class OrderMetadata {
    
    private final String fieldName;
    private final boolean ascending;
    private final OrderType type;
    
    /**
     * Types of bucket ordering supported by the converter.
     */
    public enum OrderType {
        /** Sorting by bucket key (_key) */
        KEY,
        
        /** Sorting by document count (_count) */
        COUNT,
        
        /** Sorting by sub-aggregation result */
        AGGREGATION
    }
    
    /**
     * Creates a new OrderMetadata instance.
     * 
     * @param fieldName The field name to sort by (e.g., "_key", "_count", or aggregation path)
     * @param ascending True for ascending order, false for descending
     * @param type The type of order (KEY, COUNT, or AGGREGATION)
     * @throws NullPointerException if fieldName or type is null
     */
    public OrderMetadata(String fieldName, boolean ascending, OrderType type) {
        this.fieldName = Objects.requireNonNull(fieldName, "fieldName cannot be null");
        this.ascending = ascending;
        this.type = Objects.requireNonNull(type, "type cannot be null");
    }
    
    /**
     * Returns the field name to sort by.
     * 
     * @return Field name (e.g., "_key", "_count", or aggregation path)
     */
    public String getFieldName() {
        return fieldName;
    }
    
    /**
     * Returns whether the sort is ascending.
     * 
     * @return True for ascending, false for descending
     */
    public boolean isAscending() {
        return ascending;
    }
    
    /**
     * Returns the type of order.
     * 
     * @return OrderType (KEY, COUNT, or AGGREGATION)
     */
    public OrderType getType() {
        return type;
    }
    
    @Override
    public String toString() {
        return "OrderMetadata{" +
                "fieldName='" + fieldName + '\'' +
                ", ascending=" + ascending +
                ", type=" + type +
                '}';
    }
}
