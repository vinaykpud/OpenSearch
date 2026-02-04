/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Calcite optimization rules.
 *
 * <p>Each rule converts one logical operator to its physical counterpart, or applies
 * a transformation for distributed execution.
 *
 * <h2>Converter Rules (Logical to Physical):</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.optimizer.rules.OpenSearchScanRule} -
 *       Converts LogicalTableScan to OpenSearchScan.</li>
 *   <li>{@link org.opensearch.queryplanner.optimizer.rules.OpenSearchFilterRule} -
 *       Converts LogicalFilter to OpenSearchFilter.</li>
 *   <li>{@link org.opensearch.queryplanner.optimizer.rules.OpenSearchProjectRule} -
 *       Converts LogicalProject to OpenSearchProject.</li>
 *   <li>{@link org.opensearch.queryplanner.optimizer.rules.OpenSearchAggregateRule} -
 *       Converts LogicalAggregate to OpenSearchAggregate.</li>
 *   <li>{@link org.opensearch.queryplanner.optimizer.rules.OpenSearchSortRule} -
 *       Converts LogicalSort to OpenSearchSort.</li>
 * </ul>
 *
 * <h2>Distribution Rules:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.optimizer.rules.AggregatePartialRule} -
 *       Splits FULL aggregates into PARTIAL (shards) + GATHER + FINAL (coordinator)
 *       for distributed execution.</li>
 * </ul>
 */
package org.opensearch.queryplanner.optimizer.rules;
