/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Root package for the Query Planner plugin.
 *
 * <p>Contains the plugin entry point, core interfaces, and the main coordinator
 * that orchestrates query execution from SQL parsing through distributed execution.
 *
 * <h2>Key Classes:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.QueryPlannerPlugin} - Plugin entry point.
 *       Registers REST handlers, transport actions, and wires dependencies.</li>
 *   <li>{@link org.opensearch.queryplanner.coordinator.QueryCoordinator} - Interface for orchestrating
 *       query planning and distributed execution.</li>
 *   <li>{@link org.opensearch.queryplanner.coordinator.DefaultQueryCoordinator} - Main implementation.
 *       Takes SQL, parses it, optimizes, fragments the plan, distributes to shards,
 *       and merges results.</li>
 *   <li>{@link org.opensearch.queryplanner.CalciteSqlParser} - Parses SQL strings using
 *       Apache Calcite. Produces a logical RelNode tree.</li>
 *   <li>{@link org.opensearch.queryplanner.SchemaProvider} - Interface for providing
 *       table schemas (column names and types) to the planner.</li>
 *   <li>{@link org.opensearch.queryplanner.OpenSearchSchemaFactory} - Creates Calcite
 *       schemas from OpenSearch index metadata. Maps ES field types to SQL types.</li>
 * </ul>
 */
package org.opensearch.queryplanner;
