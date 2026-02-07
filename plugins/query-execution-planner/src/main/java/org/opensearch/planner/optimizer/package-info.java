/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Query optimization components.
 * 
 * This package provides query optimization functionality using Apache Calcite's
 * optimization framework. The optimizer applies standard optimization rules such as:
 * 
 * <ul>
 *   <li>Filter pushdown - moving filters closer to data sources</li>
 *   <li>Projection pruning - eliminating unused columns</li>
 *   <li>Join reordering - optimizing join order based on costs</li>
 *   <li>Aggregate optimization - merging and removing redundant aggregations</li>
 * </ul>
 * 
 * <p>The main entry point is {@link org.opensearch.planner.optimizer.QueryOptimizer},
 * with {@link org.opensearch.planner.optimizer.CalciteQueryOptimizer} providing
 * the Calcite-based implementation.</p>
 */
package org.opensearch.planner.optimizer;
