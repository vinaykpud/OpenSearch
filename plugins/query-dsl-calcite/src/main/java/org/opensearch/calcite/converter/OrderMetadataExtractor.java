/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.converter;

import org.opensearch.calcite.exception.ConversionException;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalOrder;

/**
 * Extracts order metadata from OpenSearch BucketOrder objects.
 * 
 * This class provides type-safe extraction of field name, direction, and order type
 * from various BucketOrder implementations, replacing fragile string parsing with
 * explicit type checking.
 * 
 * Supported order types:
 * - InternalOrder.Aggregation: Sorting by sub-aggregation results
 * - Key orders: Sorting by bucket key (_key)
 * - Count orders: Sorting by document count (_count)
 */
public class OrderMetadataExtractor {
    
    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private OrderMetadataExtractor() {
        throw new UnsupportedOperationException("Utility class should not be instantiated");
    }
    
    /**
     * Extracts complete order metadata from a BucketOrder.
     * 
     * This method performs type checking to determine the order type and extracts
     * the appropriate field name and direction. All extraction happens in a single
     * pass to avoid redundant type checking.
     * 
     * @param order The BucketOrder to extract from
     * @return OrderMetadata containing field name, direction, and type
     * @throws ConversionException if order is null, unsupported, or extraction fails
     */
    public static OrderMetadata extract(BucketOrder order) throws ConversionException {
        if (order == null) {
            throw new ConversionException(
                "post-aggregation-sort",
                "BucketOrder cannot be null"
            );
        }
        
        try {
            // Try each order type in sequence
            if (order instanceof InternalOrder.Aggregation) {
                return extractAggregationOrder((InternalOrder.Aggregation) order);
            }
            
            if (InternalOrder.isKeyOrder(order)) {
                return extractKeyOrder(order);
            }
            
            if (isCountOrder(order)) {
                return extractCountOrder(order);
            }
            
            // Unknown order type
            throw new ConversionException(
                "post-aggregation-sort",
                "Unsupported BucketOrder type: " + order.getClass().getName() + 
                ". Order details: " + order.toString()
            );
            
        } catch (ConversionException e) {
            throw e; // Re-throw our own exceptions
        } catch (Exception e) {
            throw new ConversionException(
                "post-aggregation-sort",
                "Failed to extract order metadata from: " + order.toString(),
                e
            );
        }
    }
    
    /**
     * Extracts metadata from an Aggregation order.
     * 
     * Aggregation orders sort by the result of a sub-aggregation. The field name
     * is the aggregation path, and the direction is determined by comparing with
     * known ascending/descending aggregation orders.
     * 
     * @param aggOrder The aggregation order
     * @return OrderMetadata with aggregation path as field name
     * @throws ConversionException if extraction fails
     */
    private static OrderMetadata extractAggregationOrder(InternalOrder.Aggregation aggOrder) 
            throws ConversionException {
        String path = aggOrder.path().toString();
        // Create a test aggregation order with ascending=true and compare
        BucketOrder testAsc = BucketOrder.aggregation(path, true);
        boolean ascending = aggOrder.equals(testAsc);
        return new OrderMetadata(path, ascending, OrderMetadata.OrderType.AGGREGATION);
    }
    
    /**
     * Extracts metadata from a Key order.
     * 
     * Key orders sort by the bucket key. The field name is always "_key", and
     * the direction is determined by checking if it's KEY_ASC or KEY_DESC.
     * 
     * @param order The key order
     * @return OrderMetadata with "_key" as field name
     */
    private static OrderMetadata extractKeyOrder(BucketOrder order) {
        boolean ascending = InternalOrder.isKeyAsc(order);
        return new OrderMetadata("_key", ascending, OrderMetadata.OrderType.KEY);
    }
    
    /**
     * Extracts metadata from a Count order.
     * 
     * Count orders sort by document count. The field name is always "_count", and
     * the direction is determined by checking if it's COUNT_DESC (most common).
     * 
     * @param order The count order
     * @return OrderMetadata with "_count" as field name
     */
    private static OrderMetadata extractCountOrder(BucketOrder order) {
        // COUNT_DESC is the default/most common, so we check for that first
        // If it's not COUNT_DESC, it must be COUNT_ASC
        boolean ascending = !InternalOrder.isCountDesc(order);
        return new OrderMetadata("_count", ascending, OrderMetadata.OrderType.COUNT);
    }
    
    /**
     * Checks if the given order is a count order (COUNT_ASC or COUNT_DESC).
     * 
     * Since COUNT_ASC and COUNT_DESC are package-private, we check by creating
     * test orders and comparing with equals().
     * 
     * @param order The order to check
     * @return true if the order is a count order, false otherwise
     */
    private static boolean isCountOrder(BucketOrder order) {
        // Use the public BucketOrder.count() factory method to create test orders
        return order.equals(BucketOrder.count(true)) || order.equals(BucketOrder.count(false));
    }
}
