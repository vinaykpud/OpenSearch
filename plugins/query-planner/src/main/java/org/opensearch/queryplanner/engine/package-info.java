/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Native engine execution components.
 *
 * <p>This package contains the infrastructure for executing query plans
 * via native engines like DataFusion.
 *
 * <h2>Key Interfaces:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.engine.NativeEngineExecutor} - Interface for
 *       native engine execution. Takes an ExecNode subtree and returns Arrow VectorSchemaRoot.</li>
 * </ul>
 *
 * <h2>Implementations:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.engine.datafusion.DataFusionExecutor} - Executes
 *       queries via Apache DataFusion through JNI.</li>
 * </ul>
 *
 * <h2>Execution Flow:</h2>
 * <pre>
 * ExecEngine node (received on data node)
 *     │
 *     ▼
 * EngineOperator.open()
 *     │
 *     ▼
 * NativeEngineExecutor.execute(subPlan)
 *     │
 *     ▼
 * DataFusionExecutor
 *     │  - Creates DataFusion context
 *     │  - Scans data
 *     │  - Applies operations (filter, project, aggregate, etc.)
 *     │  - Collects results as Arrow IPC
 *     ▼
 * Arrow VectorSchemaRoot
 * </pre>
 *
 * @see org.opensearch.queryplanner.engine.datafusion
 */
package org.opensearch.queryplanner.engine;
