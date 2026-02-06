/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Physical operator for sorting rows.
 *
 * <p>This operator sorts its input by specified columns and sort directions.
 *
 * <p><b>Execution:</b> Typically executed by the DataFusion engine.
 */
public class SortOperator extends PhysicalOperator {

    /**
     * Represents a sort key with field name and direction.
     */
    public static class SortKey {
        private final String fieldName;
        private final boolean ascending;

        /**
         * Constructs a new SortKey.
         *
         * @param fieldName the field to sort by
         * @param ascending true for ascending, false for descending
         */
        public SortKey(String fieldName, boolean ascending) {
            this.fieldName = Objects.requireNonNull(fieldName, "fieldName cannot be null");
            this.ascending = ascending;
        }

        /**
         * Gets the field name.
         *
         * @return the field name
         */
        public String getFieldName() {
            return fieldName;
        }

        /**
         * Checks if sort is ascending.
         *
         * @return true if ascending, false if descending
         */
        public boolean isAscending() {
            return ascending;
        }

        @Override
        public String toString() {
            return fieldName + (ascending ? " ASC" : " DESC");
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            SortKey that = (SortKey) obj;
            return ascending == that.ascending && Objects.equals(fieldName, that.fieldName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, ascending);
        }
    }

    private final List<SortKey> sortKeys;

    /**
     * Constructs a new SortOperator.
     *
     * @param sortKeys the sort keys (fields and directions)
     * @param children the input operators
     * @param outputSchema the output schema (same as input)
     */
    public SortOperator(
        List<SortKey> sortKeys,
        List<PhysicalOperator> children,
        List<String> outputSchema
    ) {
        super(OperatorType.SORT, ExecutionEngine.DATAFUSION, children, outputSchema);
        this.sortKeys = sortKeys != null ? new ArrayList<>(sortKeys) : new ArrayList<>();
    }

    /**
     * Gets the sort keys.
     *
     * @return unmodifiable list of sort keys
     */
    public List<SortKey> getSortKeys() {
        return Collections.unmodifiableList(sortKeys);
    }

    @Override
    public <T> T accept(PhysicalOperatorVisitor<T> visitor) {
        return visitor.visitSort(this);
    }

    @Override
    public String toString() {
        return String.format(
            "SortOperator[sortKeys=%s, schema=%s]",
            sortKeys,
            getOutputSchema()
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        SortOperator that = (SortOperator) obj;
        return Objects.equals(sortKeys, that.sortKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sortKeys);
    }
}
