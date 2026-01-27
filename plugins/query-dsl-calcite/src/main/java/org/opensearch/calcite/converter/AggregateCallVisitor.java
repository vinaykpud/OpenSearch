/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.converter;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.calcite.exception.ConversionException;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;

import java.util.Collections;
import java.util.List;

/**
 * Implementation of AggregationBuilderVisitor that converts OpenSearch metric aggregations
 * to Calcite AggregateCall objects.
 *
 * This visitor handles the conversion of OpenSearch metric aggregation builders to their
 * Calcite equivalents, including field reference resolution and type handling.
 * 
 * Note: Bucket aggregations (like terms) are handled separately via GROUP BY in
 * CalciteConverterImpl.extractGroupByFields() and do not produce AggregateCall objects.
 */
public class AggregateCallVisitor implements AggregationBuilderVisitor {

    private final RelDataType rowType;

    /**
     * Constructor for AggregateCallVisitor.
     *
     * @param rowType The row type containing field definitions for reference lookup
     */
    public AggregateCallVisitor(RelDataType rowType) {
        this.rowType = rowType;
    }

    /**
     * Converts an AvgAggregationBuilder to a Calcite AggregateCall with AVG function.
     *
     * The avg aggregation computes the average of a numeric field. This is mapped
     * to Calcite's standard AVG aggregate function.
     *
     * @param aggregation The OpenSearch avg aggregation
     * @return Calcite AggregateCall for AVG function
     * @throws ConversionException if field lookup fails
     */
    @Override
    public AggregateCall visitAvgAggregation(AvgAggregationBuilder aggregation) throws ConversionException {
        // Get the field name from the aggregation
        String fieldName = aggregation.field();

        // Find the field index in the row type
        int fieldIndex = findFieldIndex(fieldName);

        // Get the AVG aggregate function from Calcite's standard operators
        SqlAggFunction avgFunction = SqlStdOperatorTable.AVG;

        // Create the AggregateCall
        // Arguments: function, distinct flag, approximate flag, ignored nulls flag,
        //            arg list, filter arg, collation, type, name
        return AggregateCall.create(
            avgFunction,
            false,  // not distinct
            false,  // not approximate
            false,  // don't ignore nulls
            Collections.singletonList(fieldIndex),  // field to aggregate
            -1,  // no filter
            RelCollations.EMPTY,  // empty collation instead of null
            rowType.getFieldList().get(fieldIndex).getType(),  // return type
            aggregation.getName()  // aggregation name
        );
    }

    /**
     * Finds the index of a field in the row type.
     *
     * @param fieldName The name of the field to find
     * @return The index of the field in the row type
     * @throws ConversionException if the field is not found
     */
    private int findFieldIndex(String fieldName) throws ConversionException {
        List<RelDataTypeField> fields = rowType.getFieldList();

        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(fieldName)) {
                return i;
            }
        }

        // Field not found - throw exception
        throw ConversionException.invalidField(fieldName);
    }
}
