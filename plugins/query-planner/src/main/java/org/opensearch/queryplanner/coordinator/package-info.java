/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Distributed execution coordination.
 *
 * <p>Fragments execution plans at Exchange boundaries and manages stage-based execution
 * across multiple shards.
 *
 * <h2>Key Classes:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.scheduler.StageBuilder} - Splits an
 *       ExecNode tree at Exchange boundaries into stages (LEAF for shards, ROOT for
 *       coordinator).</li>
 *   <li>{@link org.opensearch.queryplanner.scheduler.QueryStage} - Represents one
 *       stage: its ExecNode fragment, type (LEAF/ROOT), and target shards.</li>
 * </ul>
 *
 * <h2>Execution Model:</h2>
 * <pre>
 * ExecNode Tree with Exchange:
 *   Aggregate(FINAL)
 *     Exchange(GATHER)      &lt;-- Stage boundary
 *       Aggregate(PARTIAL)
 *         Scan
 *
 * Fragmented into:
 *   LEAF Stage (runs on each shard):
 *     Aggregate(PARTIAL)
 *       Scan
 *
 *   ROOT Stage (runs on coordinator):
 *     Aggregate(FINAL)
 *       [receives LEAF results]
 * </pre>
 */
package org.opensearch.queryplanner.coordinator;
