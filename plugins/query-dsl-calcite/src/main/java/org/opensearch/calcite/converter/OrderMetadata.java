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

import java.util.Objects;

/**
 * Immutable value object containing metadata extracted from a BucketOrder.
 *
 * This class holds all information needed to create a Calcite RelFieldCollation
 * from an OpenSearch BucketOrder, including the field name to sort by and the
 * sort direction.
 */
public class OrderMetadata {

    private final String fieldName;
    private final boolean ascending;

    /**
     * Creates a new OrderMetadata instance.
     *
     * @param fieldName The field name to sort by (e.g., "_key", "_count", or aggregation path)
     * @param ascending True for ascending order, false for descending
     * @throws NullPointerException if fieldName is null
     */
    public OrderMetadata(String fieldName, boolean ascending) {
        this.fieldName = Objects.requireNonNull(fieldName, "fieldName cannot be null");
        this.ascending = ascending;
    }

    /**
     * Creates an OrderMetadata from a BucketOrder by inspecting its type
     * and extracting the field name and direction.
     *
     * @param order The BucketOrder to extract from
     * @return OrderMetadata containing field name and direction
     * @throws ConversionException if order is null, unsupported, or extraction fails
     */
    public static OrderMetadata fromBucketOrder(BucketOrder order) throws ConversionException {
        if (order == null) {
            throw new ConversionException(
                "post-aggregation-sort",
                "BucketOrder cannot be null"
            );
        }

        try {
            if (order instanceof InternalOrder.Aggregation) {
                return extractAggregationOrder((InternalOrder.Aggregation) order);
            }

            if (InternalOrder.isKeyOrder(order)) {
                return extractKeyOrder(order);
            }

            if (isCountOrder(order)) {
                return extractCountOrder(order);
            }

            throw new ConversionException(
                "post-aggregation-sort",
                "Unsupported BucketOrder type: " + order.getClass().getName()
                    + ". Order details: " + order.toString()
            );

        } catch (ConversionException e) {
            throw e;
        } catch (Exception e) {
            throw new ConversionException(
                "post-aggregation-sort",
                "Failed to extract order metadata from: " + order.toString(),
                e
            );
        }
    }

    private static OrderMetadata extractAggregationOrder(InternalOrder.Aggregation aggOrder) {
        String path = aggOrder.path().toString();
        BucketOrder testAsc = BucketOrder.aggregation(path, true);
        boolean ascending = aggOrder.equals(testAsc);
        return new OrderMetadata(path, ascending);
    }

    private static OrderMetadata extractKeyOrder(BucketOrder order) {
        boolean ascending = InternalOrder.isKeyAsc(order);
        return new OrderMetadata("_key", ascending);
    }

    private static OrderMetadata extractCountOrder(BucketOrder order) {
        boolean ascending = !InternalOrder.isCountDesc(order);
        return new OrderMetadata("_count", ascending);
    }

    private static boolean isCountOrder(BucketOrder order) {
        return order.equals(BucketOrder.count(true)) || order.equals(BucketOrder.count(false));
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

    @Override
    public String toString() {
        return "OrderMetadata{"
            + "fieldName='" + fieldName + '\''
            + ", ascending=" + ascending
            + '}';
    }
}
