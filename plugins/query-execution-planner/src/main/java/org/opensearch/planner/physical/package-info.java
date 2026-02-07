/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Physical plan data model for query execution.
 *
 * <p>This package contains classes for representing physical execution plans,
 * which are concrete execution strategies with assigned execution engines.
 *
 * <p><b>Key Classes:</b>
 * <ul>
 *   <li>{@link org.opensearch.planner.physical.PhysicalPlan} - Complete physical plan with root operator</li>
 *   <li>{@link org.opensearch.planner.physical.operators.PhysicalOperator} - Abstract base for all operators</li>
 *   <li>{@link org.opensearch.planner.physical.operators.ExecutionEngine} - Enum for execution engines (Lucene, DataFusion)</li>
 *   <li>{@link org.opensearch.planner.physical.operators.OperatorType} - Enum for operator types</li>
 * </ul>
 *
 * <p><b>Concrete Operators:</b>
 * <ul>
 *   <li>{@link org.opensearch.planner.physical.operators.IndexScanOperator} - Scan Lucene index</li>
 *   <li>{@link org.opensearch.planner.physical.operators.FilterOperator} - Filter rows/documents</li>
 *   <li>{@link org.opensearch.planner.physical.operators.ProjectOperator} - Select columns/fields</li>
 *   <li>{@link org.opensearch.planner.physical.operators.HashAggregateOperator} - Hash-based aggregation</li>
 *   <li>{@link org.opensearch.planner.physical.operators.HashJoinOperator} - Hash-based join</li>
 *   <li>{@link org.opensearch.planner.physical.operators.SortOperator} - Sort rows</li>
 *   <li>{@link org.opensearch.planner.physical.operators.LimitOperator} - Limit rows</li>
 *   <li>{@link org.opensearch.planner.physical.operators.TransferOperator} - Transfer data between engines</li>
 * </ul>
 *
 * <p><b>Design Philosophy:</b>
 * Physical plans represent how queries will actually execute, with concrete
 * decisions about which execution engine handles each operation. This is in
 * contrast to logical plans, which represent what the query means semantically.
 *
 * <p><b>Example:</b>
 * <pre>
 * // Create a simple physical plan: Filter → Scan
 * IndexScanOperator scan = new IndexScanOperator(
 *     "products",
 *     Arrays.asList("name", "category", "price")
 * );
 *
 * FilterOperator filter = new FilterOperator(
 *     "price > 100",
 *     ExecutionEngine.LUCENE,
 *     Collections.singletonList(scan),
 *     Arrays.asList("name", "category", "price")
 * );
 *
 * PhysicalPlan plan = new PhysicalPlan(filter);
 * String json = plan.toJson();
 * String dot = plan.toDotGraph();
 * </pre>
 */
package org.opensearch.planner.physical;
