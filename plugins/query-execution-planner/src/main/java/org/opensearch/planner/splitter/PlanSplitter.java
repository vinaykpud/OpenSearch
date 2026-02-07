/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.splitter;

import org.opensearch.planner.physical.PhysicalPlan;

/**
 * Interface for splitting physical plans into engine-specific execution segments.
 *
 * <p>The Plan Splitter partitions a physical plan into segments that can be
 * executed by different engines (Lucene, DataFusion). It identifies engine
 * boundaries, groups consecutive same-engine operators, and builds an execution
 * graph showing dependencies between segments.
 *
 * <p><b>Key Responsibilities:</b>
 * <ul>
 *   <li>Identify engine boundaries in the physical plan</li>
 *   <li>Group consecutive same-engine operators into segments</li>
 *   <li>Insert data transfer operators at boundaries</li>
 *   <li>Build execution graph with segment dependencies</li>
 *   <li>Minimize engine transitions for efficiency</li>
 * </ul>
 *
 * <p><b>Example:</b>
 * <pre>
 * Input Physical Plan:
 *   DataFusion: HashAggregate(category, COUNT(*))
 *     Transfer(Lucene → DataFusion)
 *       Lucene: Filter(price > 100)
 *         Lucene: IndexScan(products)
 *
 * Output Split Plan:
 *   Segment 1 (Lucene):
 *     Filter(price > 100) → IndexScan(products)
 *     Dependencies: []
 *
 *   Segment 2 (DataFusion):
 *     HashAggregate(category, COUNT(*))
 *     Dependencies: [Segment 1]
 * </pre>
 */
public interface PlanSplitter {

    /**
     * Splits a physical plan into engine-specific execution segments.
     *
     * <p>This method analyzes the physical plan, identifies engine boundaries,
     * groups operators into segments, and builds an execution graph showing
     * the dependencies between segments.
     *
     * @param physicalPlan the complete physical plan to split
     * @return a split plan with segments and execution graph
     * @throws SplitException if the plan cannot be split
     */
    SplitPlan splitPlan(PhysicalPlan physicalPlan);
}
