/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Physical query optimization.
 *
 * <p>Converts logical Calcite plans to physical OpenSearch plans using cost-based
 * optimization via Calcite's VolcanoPlanner. Handles distribution traits to automatically
 * insert Exchange operators where data redistribution is needed.
 *
 * <h2>Key Classes:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.optimizer.PhysicalOptimizer} - Uses Calcite's
 *       VolcanoPlanner to convert LogicalRel to OpenSearchRel (physical operators).</li>
 *   <li>{@link org.opensearch.queryplanner.optimizer.OpenSearchDistribution} - Trait
 *       representing data distribution: SINGLETON, RANDOM, HASH, ANY.</li>
 *   <li>{@link org.opensearch.queryplanner.optimizer.OpenSearchDistributionTraitDef} -
 *       Calcite trait definition. Enables automatic Exchange insertion when distribution
 *       requirements mismatch.</li>
 * </ul>
 *
 * @see org.opensearch.queryplanner.optimizer.rules
 */
package org.opensearch.queryplanner.optimizer;
