/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical.operators;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Physical operator for filtering rows/documents based on a predicate.
 *
 * <p>This operator applies a filter condition to its input and produces
 * only the rows/documents that satisfy the condition.
 *
 * <p><b>Execution:</b> Can be executed by either Lucene (for simple predicates
 * like term queries, range queries) or DataFusion (for complex expressions).
 *
 * <p><b>Example:</b>
 * <pre>
 * FilterOperator filter = new FilterOperator(
 *     "price > 100",
 *     ExecutionEngine.LUCENE,
 *     Collections.singletonList(scanOperator),
 *     Arrays.asList("name", "category", "price")
 * );
 * </pre>
 */
public class FilterOperator extends PhysicalOperator {

    private final String predicate;

    /**
     * Constructs a new FilterOperator.
     *
     * @param predicate the filter condition (as a string expression)
     * @param executionEngine the engine to execute this filter
     * @param children the input operators
     * @param outputSchema the output schema (same as input for filters)
     */
    public FilterOperator(
        String predicate,
        ExecutionEngine executionEngine,
        List<PhysicalOperator> children,
        List<String> outputSchema
    ) {
        super(OperatorType.FILTER, executionEngine, children, outputSchema);
        this.predicate = Objects.requireNonNull(predicate, "predicate cannot be null");
    }

    /**
     * Gets the filter predicate.
     *
     * @return the predicate expression
     */
    public String getPredicate() {
        return predicate;
    }

    @Override
    public <T> T accept(PhysicalOperatorVisitor<T> visitor) {
        return visitor.visitFilter(this);
    }

    @Override
    public String toString() {
        return String.format(
            "FilterOperator[predicate=%s, engine=%s, schema=%s]",
            predicate,
            getExecutionEngine(),
            getOutputSchema()
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        FilterOperator that = (FilterOperator) obj;
        return Objects.equals(predicate, that.predicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), predicate);
    }
}
