/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assume;
import org.opensearch.datafusion.core.SessionContext;
import org.opensearch.env.Environment;
import org.opensearch.common.settings.Settings;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Unit tests for DataFusionService
 *
 * Note: These tests require the native library to be available.
 * They are disabled by default and can be enabled by setting the system property:
 * -Dtest.native.enabled=true
 */
public class DataFusionServiceTest {

    private DataFusionService service;
    private Environment testEnvironment;

    @Before
    public void setUp() throws IOException {
        // Create a temporary directory for test data
        Path tempDataDir = Files.createTempDirectory("opensearch-test-data");
        
        // Create test environment with the temp directory
        Settings settings = Settings.builder()
            .put("path.home", tempDataDir.getParent().toString())
            .put("path.data", tempDataDir.toString())
            .build();
        
        testEnvironment = new Environment(settings, null);
        
        // Create a dummy parquet file for testing
        Path parquetFile = tempDataDir.resolve("hits_data.parquet");
        Files.createFile(parquetFile);
        
        service = new DataFusionService(testEnvironment);
        service.doStart();
    }

    @Test
    public void testGetVersion() {
        String version = service.getVersion();
        assertNotNull(version);
        assertTrue(version.contains("datafusion_version"));
        assertTrue(version.contains("arrow_version"));
    }

    @Test
    public void testGetDefaultContext() {
        // Test that default context is created
        long defaultContextId = service.getDefaultContextId();
        assertTrue(defaultContextId > 0);

        // Verify context exists
        SessionContext context = service.getContext(defaultContextId);
        assertNotNull(context);

        // Close context
        boolean closed = service.closeContext(defaultContextId);
        assertTrue(closed);

        // Verify context is gone
        assertNull(service.getContext(defaultContextId));
    }
}
