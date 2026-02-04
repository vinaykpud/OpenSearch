/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * DataFusion JNI integration for native query execution.
 *
 * <p>This package provides integration with Apache DataFusion, a fast query
 * execution engine written in Rust. Queries are executed via JNI, with data
 * exchange using Arrow IPC format.
 *
 * <h2>Key Classes:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.engine.datafusion.DataFusionBridge} -
 *       JNI bridge with native method declarations for context management,
 *       DataFrame operations (filter, project, aggregate, sort, limit), and
 *       result collection.</li>
 *   <li>{@link org.opensearch.queryplanner.engine.datafusion.DataFusionExecutor} -
 *       Implementation of {@link org.opensearch.queryplanner.engine.NativeEngineExecutor}
 *       that walks ExecNode trees and translates operations to DataFusion.</li>
 * </ul>
 *
 * <h2>Native Library:</h2>
 * <p>The Rust implementation is in {@code jni/src/lib.rs}. Build with:
 * <pre>
 * cd jni
 * cargo build --release
 * </pre>
 *
 * <h2>JNI Operations:</h2>
 * <ul>
 *   <li>Context: createContext(), freeContext()</li>
 *   <li>Data source: scan() - scans a table by name</li>
 *   <li>Operations: filter(), project(), aggregate(), sort(), limit()</li>
 *   <li>Execution: collect() - returns Arrow IPC bytes</li>
 * </ul>
 *
 * <h2>Execution Flow:</h2>
 * <pre>
 * ExecNode tree
 *     ↓ DataFusionExecutor.execute()
 * DataFusion Context
 *     ↓ scan(tableName)
 * DataFrame (lazy)
 *     ↓ filter(), project(), aggregate(), etc.
 * DataFrame (lazy)
 *     ↓ collect()
 * Arrow IPC bytes
 *     ↓ deserialize
 * VectorSchemaRoot
 * </pre>
 *
 * <h2>Future: Zero-Copy via Arrow C Data Interface</h2>
 * <p>Currently uses Arrow IPC serialization. Future implementation will use
 * the Arrow C Data Interface for zero-copy data transfer between Java and Rust.
 * See IMPLEMENTATION_PLAN.md Phase 3b for details.
 */
package org.opensearch.queryplanner.engine.datafusion;
