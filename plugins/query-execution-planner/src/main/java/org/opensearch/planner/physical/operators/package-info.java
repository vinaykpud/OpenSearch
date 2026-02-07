/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Physical operator classes for query execution.
 *
 * <p>This package contains all physical operator implementations used in
 * the query execution plan. Physical operators represent concrete execution
 * steps with assigned execution engines.
 *
 * <p><b>Core Components:</b>
 * <ul>
 *   <li>{@link org.opensearch.planner.physical.operators.PhysicalOperator} - Abstract base class</li>
 *   <li>{@link org.opensearch.planner.physical.operators.ExecutionEngine} - Engine assignment enum</li>
 *   <li>{@link org.opensearch.planner.physical.operators.OperatorType} - Operator type enum</li>
 *   <li>{@link org.opensearch.planner.physical.operators.PhysicalOperatorVisitor} - Visitor pattern interface</li>
 * </ul>
 *
 * <p><b>Concrete Operators:</b>
 * <ul>
 *   <li>{@link org.opensearch.planner.physical.operators.IndexScanOperator} - Scans Lucene indexes</li>
 *   <li>{@link org.opensearch.planner.physical.operators.FilterOperator} - Filters rows/documents</li>
 *   <li>{@link org.opensearch.planner.physical.operators.ProjectOperator} - Projects columns/fields</li>
 *   <li>{@link org.opensearch.planner.physical.operators.HashAggregateOperator} - Hash-based aggregation</li>
 *   <li>{@link org.opensearch.planner.physical.operators.HashJoinOperator} - Hash-based joins</li>
 *   <li>{@link org.opensearch.planner.physical.operators.SortOperator} - Sorts rows</li>
 *   <li>{@link org.opensearch.planner.physical.operators.LimitOperator} - Limits result rows</li>
 *   <li>{@link org.opensearch.planner.physical.operators.TransferOperator} - Transfers data between engines</li>
 * </ul>
 */
package org.opensearch.planner.physical.operators;
