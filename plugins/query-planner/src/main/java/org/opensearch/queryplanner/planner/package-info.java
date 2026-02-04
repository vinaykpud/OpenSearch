/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Logical query planning.
 *
 * <p>Applies rule-based optimizations to the logical plan before physical planning.
 * Uses Calcite's HepPlanner for deterministic rule application.
 *
 * <h2>Key Classes:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.planner.QueryPlanner} - Interface for logical
 *       planners. Takes RelNode, returns optimized RelNode.</li>
 *   <li>{@link org.opensearch.queryplanner.planner.DefaultQueryPlanner} - Implementation
 *       using Calcite's HepPlanner. Applies filter pushdown and projection pruning rules.</li>
 * </ul>
 */
package org.opensearch.queryplanner.planner;
