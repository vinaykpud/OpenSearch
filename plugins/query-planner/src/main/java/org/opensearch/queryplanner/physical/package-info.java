/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Physical execution layer for the Query Planner.
 *
 * <p>This package contains everything needed to execute optimized query plans:
 * Calcite physical operators, serializable execution nodes, and runtime operators.
 *
 * <h2>Sub-packages:</h2>
 * <ul>
 *   <li>{@code physical.rel} - Calcite physical RelNode implementations. These participate
 *       in Calcite's optimizer and convert to ExecNodes for distributed execution.</li>
 *   <li>{@code physical.exec} - ExecNode classes. Serializable plan fragments that can be
 *       sent to shards via OpenSearch's transport layer.</li>
 *   <li>{@code physical.operator} - Runtime operators. Volcano-model iterators that execute
 *       ExecNodes using Apache Arrow for columnar data processing.</li>
 * </ul>
 *
 * <h2>Execution Flow:</h2>
 * <pre>
 * Calcite RelNode (rel) → ExecNode (exec) → Operator (operator)
 *   (optimization)         (serialization)    (runtime execution)
 * </pre>
 */
package org.opensearch.queryplanner.physical;
