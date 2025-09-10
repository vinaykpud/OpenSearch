/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.ConcurrentMapLong;
import org.opensearch.datafusion.core.SessionContext;
import org.opensearch.env.Environment;

import java.nio.file.Path;

/**
 * Service for managing DataFusion contexts and operations - essentially like SearchService
 */
public class DataFusionService extends AbstractLifecycleComponent {

    private final Environment environment;

    /**
     * Constructor for DataFusionService.
     * @param environment The OpenSearch environment containing path configurations
     */
    public DataFusionService(Environment environment) {
        super();
        this.environment = environment;
    }

    private static final Logger logger = LogManager.getLogger(DataFusionService.class);

    // in memory contexts, similar to ReaderContext in SearchService, just a ptr to SessionContext for now.
    private final ConcurrentMapLong<SessionContext> contexts = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();

    // Default context created at startup
    private long defaultContextId = 0;

    @Override
    protected void doStart() {
        logger.info("Starting DataFusion service");
        try {
            // Test that the native library loads correctly
            String version = DataFusionJNI.getVersion();
            logger.info("DataFusion service started successfully. Version info: {}", version);

            // Create a default context with parquet file path in OpenSearch data directory
            Path dataPath = environment.binDir(); // Use the first data directory
            Path parquetFile = dataPath.resolve("hits.parquet");
            String parquetFilePath = parquetFile.toString();

            // Check if the parquet file exists
            if (!java.nio.file.Files.exists(parquetFile)) {
                throw new RuntimeException("Parquet file not found at: " + parquetFilePath +
                    ". Please place your parquet file in the OpenSearch data directory.");
            }
            SessionContext defaultContext = new SessionContext(parquetFilePath);
            defaultContextId = defaultContext.getContextId();
            contexts.put(defaultContextId, defaultContext);
            logger.info("Created default DataFusion context with ID: {}", defaultContextId);
        } catch (Exception e) {
            logger.error("Failed to start DataFusion service", e);
            throw new RuntimeException("Failed to initialize DataFusion JNI", e);
        }
    }

    @Override
    protected void doStop() {
        logger.info("Stopping DataFusion service");
        // Close all named contexts
        for (SessionContext ctx : contexts.values()) {
            try {
                ctx.close();
            } catch (Exception e) {
                logger.warn("Error closing DataFusion context", e);
            }
        }
        contexts.clear();
        logger.info("DataFusion service stopped");
    }

    @Override
    protected void doClose() {
        // Ensure all resources are cleaned up
        doStop();
    }


    /**
     * Get a context by id
     * @param id the context id
     * @return the context ID, or null if not found
     */
    SessionContext getContext(long id) {
        return contexts.get(id);
    }

    /**
     * Close a context
     * @param contextId the context id
     * @return true if the context was found and closed, false otherwise
     */
    public boolean closeContext(long contextId) {
        try {
            SessionContext ctx = contexts.remove(contextId);
            if (ctx != null) {
                DataFusionJNI.nativeCloseContext(contextId);
                ctx.close();
                return true;
            }
        } catch (Exception e) {
            logger.error("Error closing DataFusion context", e);
            throw new RuntimeException(e);
        }
        return false;
    }

    /**
     * Get version information
     * @return JSON version string
     */
    public String getVersion() {
        return DataFusionJNI.getVersion();
    }

    /**
     * Get the default context ID created at startup
     * @return the default context ID
     */
    public long getDefaultContextId() {
        return defaultContextId;
    }

    /**
     * Execute a Substrait query plan
     * @param contextId the context ID
     * @param queryPlanIR the Substrait query plan as bytes
     * @return JSON string containing query results as array of objects
     */
    public String executeSubstraitQueryPlan(long contextId, byte[] queryPlanIR) {
        return DataFusionJNI.nativeExecuteSubstraitQueryPlan(contextId, queryPlanIR);
    }
}
