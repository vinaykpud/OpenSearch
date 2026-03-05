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
 * Null Object implementation of {@link DownstreamCapabilities} that allows everything.
 *
 * Used as the default when no real downstream system is configured,
 * such as during development, testing, or standalone use.
 */
public class AllSupportedCapabilities implements DownstreamCapabilities {

    /** Creates a capabilities instance that allows all operations. */
    public AllSupportedCapabilities() {}

    @Override
    public boolean isRelNodeSupported(Class<? extends RelNode> type) {
        return true;
    }

    @Override
    public boolean isOperatorSupported(SqlOperator operator) {
        return true;
    }

    @Override
    public boolean isAggFunctionSupported(SqlAggFunction function) {
        return true;
    }
}
