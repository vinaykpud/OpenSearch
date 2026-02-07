/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Plan splitting functionality for partitioning physical plans into execution segments.
 *
 * <p>This package provides the infrastructure for splitting physical execution plans
 * into segments that can be executed by different engines (Lucene, DataFusion).
 *
 * <p><b>Key Components:</b>
 * <ul>
 *   <li>{@link org.opensearch.planner.splitter.PlanSplitter} - Interface for plan splitting</li>
 *   <li>{@link org.opensearch.planner.splitter.DefaultPlanSplitter} - Default implementation</li>
 *   <li>{@link org.opensearch.planner.splitter.SplitPlan} - Result of splitting</li>
 *   <li>{@link org.opensearch.planner.splitter.ExecutionSegment} - Single-engine execution unit</li>
 *   <li>{@link org.opensearch.planner.splitter.ExecutionGraph} - Dependency graph</li>
 * </ul>
 *
 * <p><b>Usage Example:</b>
 * <pre>
 * PlanSplitter splitter = new DefaultPlanSplitter();
 * SplitPlan splitPlan = splitter.splitPlan(physicalPlan);
 *
 * for (ExecutionSegment segment : splitPlan.getSegments()) {
 *     System.out.println("Segment: " + segment.getSegmentId());
 *     System.out.println("Engine: " + segment.getEngine());
 *     System.out.println("Dependencies: " + segment.getDependencies());
 * }
 *
 * List&lt;String&gt; executionOrder = splitPlan.getExecutionGraph().topologicalSort();
 * </pre>
 */
package org.opensearch.planner.splitter;
