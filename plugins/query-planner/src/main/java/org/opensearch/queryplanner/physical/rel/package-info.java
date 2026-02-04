/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Calcite physical relational operators.
 *
 * <p>These are Calcite RelNode implementations that represent our physical execution
 * plan within Calcite's optimizer. Each implements {@link org.opensearch.queryplanner.physical.rel.OpenSearchRel}
 * and provides a {@code toExecNode()} method for conversion to our execution format.
 *
 * <h2>Key Classes:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.physical.rel.OpenSearchRel} - Marker interface. All
 *       physical operators implement this and provide toExecNode() for conversion.</li>
 *   <li>{@link org.opensearch.queryplanner.physical.rel.OpenSearchConvention} - Calcite Convention
 *       that identifies our physical operators.</li>
 *   <li>{@link org.opensearch.queryplanner.physical.rel.OpenSearchScan} - Physical table scan.
 *       Reads from an OpenSearch index.</li>
 *   <li>{@link org.opensearch.queryplanner.physical.rel.OpenSearchFilter} - Physical filter.
 *       Applies predicates to rows.</li>
 *   <li>{@link org.opensearch.queryplanner.physical.rel.OpenSearchProject} - Physical projection.
 *       Selects/computes columns.</li>
 *   <li>{@link org.opensearch.queryplanner.physical.rel.OpenSearchAggregate} - Physical aggregation.
 *       Supports PARTIAL/FINAL/FULL modes for distributed aggregation.</li>
 *   <li>{@link org.opensearch.queryplanner.physical.rel.OpenSearchSort} - Physical sort with
 *       optional LIMIT.</li>
 *   <li>{@link org.opensearch.queryplanner.physical.rel.OpenSearchLimit} - Physical limit operator.</li>
 *   <li>{@link org.opensearch.queryplanner.physical.rel.OpenSearchExchange} - Physical exchange
 *       (data shuffle). Marks stage boundaries in distributed execution.</li>
 * </ul>
 */
package org.opensearch.queryplanner.physical.rel;
