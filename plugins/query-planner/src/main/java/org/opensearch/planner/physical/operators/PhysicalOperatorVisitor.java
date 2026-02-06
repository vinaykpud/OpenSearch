/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical.operators;

/**
 * Visitor interface for traversing physical operator trees.
 *
 * <p>This interface implements the Visitor pattern, allowing external code
 * to process physical operators without modifying the operator classes themselves.
 *
 * <p><b>Common Use Cases:</b>
 * <ul>
 *   <li>Plan serialization (JSON, DOT graph)</li>
 *   <li>Plan validation</li>
 *   <li>Cost estimation</li>
 *   <li>Plan transformation</li>
 *   <li>Execution</li>
 * </ul>
 *
 * <p><b>Example Usage:</b>
 * <pre>
 * PhysicalOperatorVisitor&lt;String&gt; jsonSerializer = new JsonSerializerVisitor();
 * String json = physicalPlan.getRoot().accept(jsonSerializer);
 * </pre>
 *
 * @param <T> the return type of the visitor methods
 */
public interface PhysicalOperatorVisitor<T> {

    /**
     * Visits an IndexScanOperator.
     *
     * @param operator the index scan operator
     * @return the result of visiting this operator
     */
    T visitIndexScan(IndexScanOperator operator);

    /**
     * Visits a FilterOperator.
     *
     * @param operator the filter operator
     * @return the result of visiting this operator
     */
    T visitFilter(FilterOperator operator);

    /**
     * Visits a ProjectOperator.
     *
     * @param operator the project operator
     * @return the result of visiting this operator
     */
    T visitProject(ProjectOperator operator);

    /**
     * Visits a HashAggregateOperator.
     *
     * @param operator the hash aggregate operator
     * @return the result of visiting this operator
     */
    T visitHashAggregate(HashAggregateOperator operator);

    /**
     * Visits a HashJoinOperator.
     *
     * @param operator the hash join operator
     * @return the result of visiting this operator
     */
    T visitHashJoin(HashJoinOperator operator);

    /**
     * Visits a SortOperator.
     *
     * @param operator the sort operator
     * @return the result of visiting this operator
     */
    T visitSort(SortOperator operator);

    /**
     * Visits a LimitOperator.
     *
     * @param operator the limit operator
     * @return the result of visiting this operator
     */
    T visitLimit(LimitOperator operator);

    /**
     * Visits a TransferOperator.
     *
     * @param operator the transfer operator
     * @return the result of visiting this operator
     */
    T visitTransfer(TransferOperator operator);
}
