/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Basic tests for QueryPlannerPlugin.
 *
 * This is a placeholder test class to satisfy OpenSearch's testing conventions.
 * Actual tests will be added as we implement features in subsequent tasks.
 */
public class QueryPlannerPluginTests extends OpenSearchTestCase {

    /**
     * Test that the plugin can be instantiated.
     */
    public void testPluginInstantiation() {
        QueryPlannerPlugin plugin = new QueryPlannerPlugin();
        assertNotNull(plugin);
    }
}
