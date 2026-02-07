/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical.operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Physical operator for hash-based aggregation.
 *
 * <p>This operator performs GROUP BY aggregations using hash tables.
 * It is best suited for queries with many groups or unsorted input.
 *
 * <p><b>Execution:</b> Always executed by the DataFusion engine.
 *
 * <p><b>Example:</b>
 * <pre>
 * HashAggregateOperator agg = new HashAggregateOperator(
 *     Arrays.asList("category"),           // Group by category
 *     Arrays.asList("COUNT(*)", "SUM(price)"), // Aggregate functions
 *     Collections.singletonList(filterOperator),
 *     Arrays.asList("category", "count", "total_price")
 * );
 * </pre>
 */
public class HashAggregateOperator extends PhysicalOperator {

    private final List<String> groupByFields;
    private final List<String> aggregateFunctions;

    /**
     * Constructs a new HashAggregateOperator.
     *
     * @param groupByFields the fields to group by
     * @param aggregateFunctions the aggregate functions to apply
     * @param children the input operators
     * @param outputSchema the output schema (group fields + aggregate results)
     */
    public HashAggregateOperator(
        List<String> groupByFields,
        List<String> aggregateFunctions,
        List<PhysicalOperator> children,
        List<String> outputSchema
    ) {
        super(OperatorType.HASH_AGGREGATE, ExecutionEngine.DATAFUSION, children, outputSchema);
        this.groupByFields = groupByFields != null ? new ArrayList<>(groupByFields) : new ArrayList<>();
        this.aggregateFunctions = aggregateFunctions != null ? new ArrayList<>(aggregateFunctions) : new ArrayList<>();
    }

    /**
     * Gets the fields to group by.
     *
     * @return unmodifiable list of group by fields
     */
    public List<String> getGroupByFields() {
        return Collections.unmodifiableList(groupByFields);
    }

    /**
     * Gets the aggregate functions.
     *
     * @return unmodifiable list of aggregate functions
     */
    public List<String> getAggregateFunctions() {
        return Collections.unmodifiableList(aggregateFunctions);
    }

    @Override
    public <T> T accept(PhysicalOperatorVisitor<T> visitor) {
        return visitor.visitHashAggregate(this);
    }

    @Override
    public String toString() {
        return String.format(
            "HashAggregateOperator[groupBy=%s, aggregates=%s, schema=%s]",
            groupByFields,
            aggregateFunctions,
            getOutputSchema()
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        HashAggregateOperator that = (HashAggregateOperator) obj;
        return Objects.equals(groupByFields, that.groupByFields)
            && Objects.equals(aggregateFunctions, that.aggregateFunctions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), groupByFields, aggregateFunctions);
    }
}
