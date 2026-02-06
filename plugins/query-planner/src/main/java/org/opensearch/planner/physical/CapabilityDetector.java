/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;

/**
 * Interface for detecting which execution engine can handle specific operations.
 *
 * <p>The capability detector analyzes logical operators and determines whether
 * they can be executed by Lucene, DataFusion, or require a hybrid approach.
 *
 * <p><b>Design Philosophy:</b>
 * Use Lucene for what it does best (inverted index operations) and DataFusion
 * for complex analytical operations. This maximizes performance by leveraging
 * the strengths of each engine.
 *
 * <p><b>Example Usage:</b>
 * <pre>
 * CapabilityDetector detector = new DefaultCapabilityDetector();
 * ExecutionEngine engine = detector.detectEngine(logicalFilter);
 * // Returns LUCENE for simple filters, DATAFUSION for complex ones
 * </pre>
 */
public interface CapabilityDetector {

    /**
     * Detects which execution engine should handle a logical operator.
     *
     * <p>This is the main entry point for engine assignment. It analyzes
     * the operator type and its properties to determine the best engine.
     *
     * @param operator the logical operator to analyze
     * @return the recommended execution engine
     */
    ExecutionEngine detectEngine(RelNode operator);

    /**
     * Checks if Lucene can execute a filter operation.
     *
     * <p>Lucene can handle:
     * <ul>
     *   <li>Term queries (exact matches)</li>
     *   <li>Range queries (numeric, date ranges)</li>
     *   <li>Boolean queries (AND, OR, NOT combinations)</li>
     *   <li>Prefix queries</li>
     *   <li>Wildcard queries</li>
     * </ul>
     *
     * <p>Lucene cannot handle:
     * <ul>
     *   <li>Complex expressions (mathematical operations)</li>
     *   <li>User-defined functions</li>
     *   <li>Subqueries</li>
     * </ul>
     *
     * @param filter the filter operator
     * @return true if Lucene can execute this filter
     */
    boolean canLuceneExecuteFilter(Filter filter);

    /**
     * Checks if Lucene can execute a projection operation.
     *
     * <p>Lucene can handle:
     * <ul>
     *   <li>Stored fields retrieval</li>
     *   <li>DocValues retrieval</li>
     *   <li>Simple field selection</li>
     * </ul>
     *
     * <p>Lucene cannot handle:
     * <ul>
     *   <li>Computed columns (expressions)</li>
     *   <li>Complex transformations</li>
     * </ul>
     *
     * @param project the project operator
     * @return true if Lucene can execute this projection
     */
    boolean canLuceneExecuteProject(Project project);

    /**
     * Checks if a RexNode expression can be executed by Lucene.
     *
     * <p>This is used to analyze filter conditions and projection expressions.
     *
     * @param expression the expression to analyze
     * @return true if Lucene can execute this expression
     */
    boolean canLuceneExecuteExpression(RexNode expression);

    /**
     * Checks if Lucene can execute a table scan.
     *
     * <p>Lucene can always scan its own indexes, so this typically returns true.
     *
     * @param tableScan the table scan operator
     * @return true if Lucene can execute this scan
     */
    boolean canLuceneExecuteTableScan(TableScan tableScan);

    /**
     * Checks if an aggregation should be executed by DataFusion.
     *
     * <p>Aggregations are always assigned to DataFusion because Lucene
     * doesn't have built-in aggregation capabilities.
     *
     * @param aggregate the aggregate operator
     * @return true (always, aggregations go to DataFusion)
     */
    boolean shouldDataFusionExecuteAggregate(Aggregate aggregate);

    /**
     * Checks if a join should be executed by DataFusion.
     *
     * <p>Joins are always assigned to DataFusion because Lucene
     * doesn't have built-in join capabilities.
     *
     * @param join the join operator
     * @return true (always, joins go to DataFusion)
     */
    boolean shouldDataFusionExecuteJoin(Join join);

    /**
     * Checks if a sort should be executed by DataFusion.
     *
     * <p>Sorts are typically assigned to DataFusion for efficient
     * sorting algorithms, though Lucene can do simple sorting.
     *
     * @param sort the sort operator
     * @return true if DataFusion should execute this sort
     */
    boolean shouldDataFusionExecuteSort(Sort sort);
}
