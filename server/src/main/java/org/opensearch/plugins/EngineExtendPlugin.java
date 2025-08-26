/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

/**
 * Plugin interface for extending OpenSearch engine functionality.
 * This interface allows plugins to extend the core engine capabilities.
 */
public interface EngineExtendPlugin {
    /**
     * Execute engine extension operations.
     *
     * @param queryPlanIR queryPlan Intermediate Representation
     */
    default void execute(byte[] queryPlanIR) {}
}
