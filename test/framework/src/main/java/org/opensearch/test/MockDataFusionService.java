/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.plugins.EngineExtendPlugin;
import org.opensearch.plugins.Plugin;

/**
 * Mock DataFusion service for testing purposes.
 * This service provides a test implementation that doesn't require native JNI libraries.
 */
public class MockDataFusionService {

    private static final Logger logger = LogManager.getLogger(MockDataFusionService.class);

    /**
     * Test plugin that extends EngineExtendPlugin for DataFusion mock functionality.
     */
    public static class TestPlugin extends Plugin implements EngineExtendPlugin {
        @Override
        public void execute(byte[] queryPlanIR) {
            // Mock implementation - just log the execution
            if (queryPlanIR != null) {
                logger.debug("Mock DataFusion executing query plan IR of {} bytes", queryPlanIR.length);
            }
        }
    }
}
