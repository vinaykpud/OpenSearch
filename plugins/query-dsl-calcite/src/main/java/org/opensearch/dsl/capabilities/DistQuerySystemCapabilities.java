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
 * Adapter that delegates capability checks to a downstream DistQuerySystem.
 *
 * The DistQuerySystem is referenced by a simple capability-checking interface
 * to avoid a compile-time dependency on the downstream module.
 */
public class DistQuerySystemCapabilities implements DownstreamCapabilities {

    /**
     * Functional interface for checking whether the downstream supports a given name.
     */
    @FunctionalInterface
    public interface SupportChecker {
        boolean isSupported(String name);
    }

    private final SupportChecker checker;

    public DistQuerySystemCapabilities(SupportChecker checker) {
        this.checker = checker;
    }

    @Override
    public boolean isRelNodeSupported(Class<? extends RelNode> type) {
        return checker.isSupported(type.getSimpleName());
    }

    @Override
    public boolean isOperatorSupported(SqlOperator operator) {
        return checker.isSupported(operator.getName());
    }

    @Override
    public boolean isAggFunctionSupported(SqlAggFunction function) {
        return checker.isSupported(function.getName());
    }
}
