/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Capability detection for execution engine assignment.
 *
 * <p>This package contains classes for analyzing logical operators and determining
 * which execution engine (Lucene or DataFusion) should execute them.
 *
 * <p><b>Key Classes:</b>
 * <ul>
 *   <li>{@link org.opensearch.planner.physical.capabilitydetector.CapabilityDetector} - Interface for capability detection</li>
 *   <li>{@link org.opensearch.planner.physical.capabilitydetector.DefaultCapabilityDetector} - Default implementation</li>
 * </ul>
 *
 * <p><b>Design Philosophy:</b>
 * The capability detector uses a simple heuristic approach:
 * <ul>
 *   <li>Use Lucene for what it does best: inverted index operations (scans, simple filters, simple projections)</li>
 *   <li>Use DataFusion for complex analytical operations (aggregations, joins, complex expressions)</li>
 *   <li>Insert Transfer operators at engine boundaries to move data between engines</li>
 * </ul>
 *
 * <p><b>Example Usage:</b>
 * <pre>
 * CapabilityDetector detector = new DefaultCapabilityDetector();
 * ExecutionEngine engine = detector.detectEngine(logicalFilter);
 * if (engine == ExecutionEngine.LUCENE) {
 *     // Execute with Lucene
 * } else {
 *     // Execute with DataFusion
 * }
 * </pre>
 */
package org.opensearch.planner.physical.capabilitydetector;
