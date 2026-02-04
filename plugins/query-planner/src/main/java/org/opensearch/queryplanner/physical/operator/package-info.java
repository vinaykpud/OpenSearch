/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Runtime operators for query execution.
 *
 * <p>These execute the plan at runtime using Apache Arrow vectors for columnar data.
 * All operators implement the Volcano iterator model: open(), next(), close().
 *
 * <h2>Operator Interface:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.physical.operator.Operator} - Base interface
 *       with open(), next(), close() methods.</li>
 * </ul>
 *
 * <h2>Core Operators:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.physical.operator.ScanOperator} - Reads data
 *       from a DataProvider (abstraction over data source). Returns Arrow batches.</li>
 *   <li>{@link org.opensearch.queryplanner.physical.operator.AggregateOperator} - Groups
 *       rows by key, applies accumulators (SUM, COUNT, MIN, MAX). Handles PARTIAL/FINAL modes.</li>
 *   <li>{@link org.opensearch.queryplanner.physical.operator.EngineOperator} - Delegates
 *       execution to a native engine (e.g., DataFusion) via NativeEngineExecutor.</li>
 * </ul>
 *
 * <h2>Distributed Execution Support:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.physical.operator.GatheredDataOperator} -
 *       Source operator for ROOT stage. Provides pre-gathered shard results as input
 *       to FINAL operators.</li>
 * </ul>
 *
 * <h2>Lucene Integration:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.physical.operator.LuceneDataProvider} -
 *       DataProvider implementation that reads from Lucene via DocValuesCollector.</li>
 *   <li>{@link org.opensearch.queryplanner.physical.operator.DocValuesCollector} -
 *       Lucene Collector that reads doc values for columns and converts to Arrow vectors.</li>
 * </ul>
 *
 * <h2>Factory:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.physical.operator.OperatorFactory} - Converts
 *       ExecNode tree to Operator tree. Given an ExecNode and DataProvider, produces
 *       runnable operators.</li>
 * </ul>
 *
 * <h2>Native Engine Integration:</h2>
 * <p>Most query operations (filter, project, sort, limit) are delegated to the native
 * engine (DataFusion) via EngineOperator. This package provides:
 * <ul>
 *   <li>ScanOperator for reading from Lucene/DocValues</li>
 *   <li>AggregateOperator for FINAL aggregation at coordinator</li>
 *   <li>EngineOperator for delegating to native engine</li>
 * </ul>
 */
package org.opensearch.queryplanner.physical.operator;
