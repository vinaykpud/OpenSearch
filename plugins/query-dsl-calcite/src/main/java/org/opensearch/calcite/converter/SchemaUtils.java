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
import org.opensearch.calcite.exception.ConversionException;

import java.util.List;

/**
 * Utility class for schema-related operations in the Calcite converter.
 * 
 * This class provides common helper methods for working with Calcite RelDataType
 * schemas, including field lookups and validation.
 */
public final class SchemaUtils {

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private SchemaUtils() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }

    /**
     * Finds the index of a field in the row type.
     *
     * @param fieldName The name of the field to find
     * @param rowType The row type containing field definitions
     * @return The index of the field in the row type
     * @throws ConversionException if the field is not found
     */
    public static int findFieldIndex(String fieldName, RelDataType rowType) throws ConversionException {
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
