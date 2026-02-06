/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical;

/**
 * Enum representing the type of physical operator in a query plan.
 *
 * <p>Physical operators represent concrete execution steps, as opposed to
 * logical operators which represent abstract query semantics.
 *
 * <p><b>Operator Categories:</b>
 * <ul>
 *   <li><b>Scan Operators</b>: INDEX_SCAN</li>
 *   <li><b>Filter Operators</b>: FILTER</li>
 *   <li><b>Projection Operators</b>: PROJECT</li>
 *   <li><b>Aggregation Operators</b>: HASH_AGGREGATE, SORT_AGGREGATE</li>
 *   <li><b>Join Operators</b>: HASH_JOIN, NESTED_LOOP_JOIN, MERGE_JOIN</li>
 *   <li><b>Sort Operators</b>: SORT</li>
 *   <li><b>Limit Operators</b>: LIMIT</li>
 *   <li><b>Data Transfer Operators</b>: TRANSFER</li>
 * </ul>
 */
public enum OperatorType {
    /**
     * Index scan operator - reads documents from a Lucene index.
     *
     * <p>Typically executed by Lucene engine.
     */
    INDEX_SCAN,

    /**
     * Filter operator - applies predicates to filter rows/documents.
     *
     * <p>Can be executed by either Lucene (for simple predicates) or
     * DataFusion (for complex expressions).
     */
    FILTER,

    /**
     * Project operator - selects specific columns/fields.
     *
     * <p>Can be executed by either engine depending on context.
     */
    PROJECT,

    /**
     * Hash aggregate operator - performs aggregations using hash tables.
     *
     * <p>Typically executed by DataFusion engine.
     * <p>Best for: GROUP BY with many groups, unsorted input.
     */
    HASH_AGGREGATE,

    /**
     * Sort aggregate operator - performs aggregations on sorted input.
     *
     * <p>Typically executed by DataFusion engine.
     * <p>Best for: GROUP BY with few groups, already sorted input.
     */
    SORT_AGGREGATE,

    /**
     * Hash join operator - performs joins using hash tables.
     *
     * <p>Executed by DataFusion engine.
     * <p>Best for: Equi-joins, large datasets.
     */
    HASH_JOIN,

    /**
     * Nested loop join operator - performs joins using nested loops.
     *
     * <p>Executed by DataFusion engine.
     * <p>Best for: Small datasets, non-equi joins.
     */
    NESTED_LOOP_JOIN,

    /**
     * Merge join operator - performs joins on sorted inputs.
     *
     * <p>Executed by DataFusion engine.
     * <p>Best for: Already sorted inputs, large datasets.
     */
    MERGE_JOIN,

    /**
     * Sort operator - sorts rows by specified columns.
     *
     * <p>Typically executed by DataFusion engine.
     */
    SORT,

    /**
     * Limit operator - limits the number of rows returned.
     *
     * <p>Can be executed by either engine.
     */
    LIMIT,

    /**
     * Transfer operator - transfers data between execution engines.
     *
     * <p>Converts data from one engine's format to another.
     * For example: Lucene documents → Arrow RecordBatch.
     */
    TRANSFER
}
