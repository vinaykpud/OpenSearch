/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.dsl.exception.ConversionException;

import java.util.ArrayList;
import java.util.List;

/**
 * Field-based grouping: GROUP BY field1, field2, ...
 *
 * Used by terms and multi_terms bucket aggregations where the GROUP BY
 * columns are direct field references (not computed expressions).
 */
public class FieldGrouping implements GroupingInfo {

    private final List<String> fieldNames;

    public FieldGrouping(List<String> fieldNames) {
        this.fieldNames = List.copyOf(fieldNames);
    }

    @Override
    public List<String> getFieldNames() {
        return fieldNames;
    }

    @Override
    public List<Integer> resolveIndices(RelDataType inputRowType) throws ConversionException {
        List<Integer> indices = new ArrayList<>(fieldNames.size());
        for (String name : fieldNames) {
            RelDataTypeField field = inputRowType.getField(name, true, false);
            if (field == null) {
                throw ConversionException.invalidField(name);
            }
            indices.add(field.getIndex());
        }
        return indices;
    }
}
