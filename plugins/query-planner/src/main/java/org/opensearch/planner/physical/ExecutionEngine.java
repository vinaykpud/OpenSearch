/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical;

/**
 * Enum representing the execution engine for physical operators.
 *
 * <p>This enum identifies which execution engine should handle a particular
 * physical operator in the query plan.
 *
 * <p><b>Execution Engines:</b>
 * <ul>
 *   <li><b>LUCENE</b>: Apache Lucene engine for inverted index operations
 *       (term queries, range queries, boolean queries, field retrieval)</li>
 *   <li><b>DATAFUSION</b>: Apache Arrow DataFusion engine for analytical operations
 *       (aggregations, joins, complex filters, window functions)</li>
 *   <li><b>HYBRID</b>: Combination of both engines with data transfer between them</li>
 * </ul>
 *
 * <p><b>Design Philosophy:</b>
 * Use Lucene for what it does best (inverted index lookups) and DataFusion
 * for complex analytical operations. This hybrid approach maximizes performance
 * by leveraging the strengths of each engine.
 */
public enum ExecutionEngine {
    /**
     * Apache Lucene execution engine.
     *
     * <p>Best for:
     * <ul>
     *   <li>Term queries (exact matches)</li>
     *   <li>Range queries (numeric, date ranges)</li>
     *   <li>Boolean queries (AND, OR, NOT combinations)</li>
     *   <li>Field retrieval (stored fields, doc values)</li>
     *   <li>Full-text search</li>
     * </ul>
     */
    LUCENE,

    /**
     * Apache Arrow DataFusion execution engine.
     *
     * <p>Best for:
     * <ul>
     *   <li>Aggregations (GROUP BY, COUNT, SUM, AVG, etc.)</li>
     *   <li>Joins (INNER, LEFT, RIGHT, FULL)</li>
     *   <li>Complex filters (expressions, functions)</li>
     *   <li>Window functions</li>
     *   <li>Sorting and limiting</li>
     * </ul>
     */
    DATAFUSION,

    /**
     * Hybrid execution using both Lucene and DataFusion.
     *
     * <p>Used when a query requires operations from both engines,
     * with data transfer between them. For example:
     * <ul>
     *   <li>Lucene filters documents, DataFusion aggregates results</li>
     *   <li>Lucene retrieves fields, DataFusion performs joins</li>
     * </ul>
     */
    HYBRID
}
