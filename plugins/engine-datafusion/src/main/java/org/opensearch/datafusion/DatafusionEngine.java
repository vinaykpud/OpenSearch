/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.datafusion.core.SessionContext;
import org.opensearch.index.engine.SearchExecutionEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * DataFusion search execution engine implementation that executes Substrait query plans
 * using the DataFusion query engine for OpenSearch.
 */
public class DatafusionEngine implements SearchExecutionEngine {

    private static final Logger logger = LogManager.getLogger(DatafusionEngine.class);
    private final DataFusionService dataFusionService;

    /**
     * Constructs a new DatafusionEngine with the specified DataFusion service.
     *
     * @param dataFusionService the DataFusion service used for query execution
     */
    public DatafusionEngine(DataFusionService dataFusionService) {
        this.dataFusionService = dataFusionService;
    }

    @Override
    public ArrayList<Map<String, Object>> execute(byte[] queryPlanIR) {
        logger.info("Executing queryPlanIR: {}", queryPlanIR);
        logger.info("Substrait plan serialized to " + queryPlanIR.length + " bytes");
        ArrayList<Map<String, Object>> finalRes = new ArrayList<>();
        try {
            SessionContext defaultSessionContext = dataFusionService.getDefaultContext();
            logger.info("Query execution result stream:");
            long streamPointer = dataFusionService.executeSubstraitQueryStream(queryPlanIR);
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            RecordBatchStream stream = new RecordBatchStream(defaultSessionContext, streamPointer, allocator);
            VectorSchemaRoot root = stream.getVectorSchemaRoot();
            while (stream.loadNextBatch().join()) {
                System.out.println(root.getSchema());
                System.out.println(root.getFieldVectors());
             }

        } catch (Exception exception) {
            logger.error("Failed to execute Substrait query plan", exception);
        }
        return finalRes;
    }
}
