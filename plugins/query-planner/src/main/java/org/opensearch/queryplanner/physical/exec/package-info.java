/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Execution plan nodes (ExecNode) - serializable plan fragments.
 *
 * <p>ExecNodes are the wire format for query plans. They can be serialized via
 * OpenSearch's Writeable interface and sent to data nodes for execution.
 * Each ExecNode corresponds to a physical operator but contains only the
 * configuration needed to reconstruct that operator on the receiving node.
 *
 * <h2>Key Classes:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.physical.exec.ExecNode} - Base interface.
 *       Defines node type enum and factory method for deserialization.</li>
 *   <li>{@link org.opensearch.queryplanner.physical.exec.RelToExecConverter} - Walks the
 *      physical RelNode tree and calls toExecNode() on each operator to produce an
 *      ExecNode tree ready for distributed execution.</li>
 * </ul>
 *
 * <h2>Scan Nodes:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.physical.exec.ExecScan} - Table scan from Lucene.
 *       Contains index name, columns to read, optional filter for pushdown.</li>
 *   <li>{@link org.opensearch.queryplanner.physical.exec.ExecNativeScan} - Native engine scan.
 *       Used when the native engine handles data reading directly.</li>
 * </ul>
 *
 * <h2>Operation Nodes:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.physical.exec.ExecFilter} - Filter.
 *       Contains predicate expression applied to child output.</li>
 *   <li>{@link org.opensearch.queryplanner.physical.exec.ExecProject} - Projection.
 *       Contains list of columns to output.</li>
 *   <li>{@link org.opensearch.queryplanner.physical.exec.ExecAggregate} - Aggregation.
 *       Contains group-by columns, aggregate functions, mode (PARTIAL/FINAL/FULL).</li>
 *   <li>{@link org.opensearch.queryplanner.physical.exec.ExecSort} - Sort.
 *       Contains sort columns, directions, optional limit and offset.</li>
 *   <li>{@link org.opensearch.queryplanner.physical.exec.ExecLimit} - Limit.
 *       Contains row count limit and optional offset.</li>
 * </ul>
 *
 * <h2>Distribution Nodes:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.physical.exec.ExecExchange} - Exchange.
 *       Marks distribution boundary (GATHER, HASH, BROADCAST). Used to split
 *       plans into stages for distributed execution.</li>
 * </ul>
 *
 * <h2>Native Engine Node:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.physical.exec.ExecEngine} - Wraps a subtree
 *       for native engine execution. Contains the subPlan that will be executed
 *       by DataFusion or another native engine.</li>
 * </ul>
 */
package org.opensearch.queryplanner.physical.exec;
