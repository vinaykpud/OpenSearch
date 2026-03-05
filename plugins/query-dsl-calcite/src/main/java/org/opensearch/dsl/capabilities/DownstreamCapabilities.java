/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.capabilities;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;

/**
 * Declares what the downstream execution engine can handle.
 *
 * The planner validates against these capabilities before producing the plan,
 * so that unsupported constructs fail at plan time rather than execution time.
 */
public interface DownstreamCapabilities {

    /** Can the downstream handle this type of logical operator? */
    boolean isRelNodeSupported(Class<? extends RelNode> type);

    /** Can the downstream handle this SQL operator? (=, >=, AND, OR, FLOOR, etc.) */
    boolean isOperatorSupported(SqlOperator operator);

    /** Can the downstream handle this aggregate function? (AVG, SUM, etc.) */
    boolean isAggFunctionSupported(SqlAggFunction function);
}
