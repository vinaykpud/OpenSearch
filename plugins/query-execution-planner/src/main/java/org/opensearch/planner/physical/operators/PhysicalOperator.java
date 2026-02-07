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
 * Abstract base class for physical operators in a query execution plan.
 *
 * <p>Physical operators represent concrete execution steps with assigned
 * execution engines. They form a tree structure where each operator can
 * have zero or more child operators.
 *
 * <p><b>Key Concepts:</b>
 * <ul>
 *   <li><b>Operator Type</b>: The kind of operation (scan, filter, aggregate, etc.)</li>
 *   <li><b>Execution Engine</b>: Which engine executes this operator (Lucene, DataFusion)</li>
 *   <li><b>Children</b>: Input operators that feed data to this operator</li>
 *   <li><b>Output Schema</b>: The fields/columns produced by this operator</li>
 * </ul>
 *
 * <p><b>Tree Structure:</b>
 * Physical operators form a tree where:
 * <ul>
 *   <li>Leaf nodes are typically INDEX_SCAN operators</li>
 *   <li>Internal nodes are operators that transform data (FILTER, PROJECT, etc.)</li>
 *   <li>Root node is the final operator that produces query results</li>
 * </ul>
 *
 * <p><b>Example Tree:</b>
 * <pre>
 * HASH_AGGREGATE (DataFusion)
 *   └─ TRANSFER (Lucene → DataFusion)
 *       └─ FILTER (Lucene)
 *           └─ INDEX_SCAN (Lucene)
 * </pre>
 */
public abstract class PhysicalOperator {

    private final OperatorType operatorType;
    private final ExecutionEngine executionEngine;
    private final List<PhysicalOperator> children;
    private final List<String> outputSchema;

    /**
     * Constructs a new PhysicalOperator.
     *
     * @param operatorType the type of this operator
     * @param executionEngine the execution engine for this operator
     * @param children the child operators (inputs to this operator)
     * @param outputSchema the output schema (field names)
     */
    protected PhysicalOperator(
        OperatorType operatorType,
        ExecutionEngine executionEngine,
        List<PhysicalOperator> children,
        List<String> outputSchema
    ) {
        this.operatorType = Objects.requireNonNull(operatorType, "operatorType cannot be null");
        this.executionEngine = Objects.requireNonNull(executionEngine, "executionEngine cannot be null");
        this.children = children != null ? new ArrayList<>(children) : new ArrayList<>();
        this.outputSchema = outputSchema != null ? new ArrayList<>(outputSchema) : new ArrayList<>();
    }

    /**
     * Gets the operator type.
     *
     * @return the operator type
     */
    public OperatorType getOperatorType() {
        return operatorType;
    }

    /**
     * Gets the execution engine assigned to this operator.
     *
     * @return the execution engine
     */
    public ExecutionEngine getExecutionEngine() {
        return executionEngine;
    }

    /**
     * Gets the child operators (inputs to this operator).
     *
     * @return unmodifiable list of child operators
     */
    public List<PhysicalOperator> getChildren() {
        return Collections.unmodifiableList(children);
    }

    /**
     * Gets the output schema (field names produced by this operator).
     *
     * @return unmodifiable list of field names
     */
    public List<String> getOutputSchema() {
        return Collections.unmodifiableList(outputSchema);
    }

    /**
     * Accepts a visitor for the visitor pattern.
     *
     * <p>This allows external code to traverse and process the operator tree
     * without modifying the operator classes.
     *
     * @param visitor the visitor to accept
     * @param <T> the return type of the visitor
     * @return the result of visiting this operator
     */
    public abstract <T> T accept(PhysicalOperatorVisitor<T> visitor);

    /**
     * Returns a string representation of this operator for debugging.
     *
     * @return string representation
     */
    @Override
    public String toString() {
        return String.format(
            "%s[type=%s, engine=%s, children=%d, schema=%s]",
            getClass().getSimpleName(),
            operatorType,
            executionEngine,
            children.size(),
            outputSchema
        );
    }

    /**
     * Checks equality based on operator type, engine, children, and schema.
     *
     * @param obj the object to compare
     * @return true if equal, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PhysicalOperator that = (PhysicalOperator) obj;
        return operatorType == that.operatorType
            && executionEngine == that.executionEngine
            && Objects.equals(children, that.children)
            && Objects.equals(outputSchema, that.outputSchema);
    }

    /**
     * Computes hash code based on operator type, engine, children, and schema.
     *
     * @return hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(operatorType, executionEngine, children, outputSchema);
    }
}
