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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.SearchExecutionEngine;

import java.util.ArrayList;
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
            // Use the default context created at service startup
            long defaultContextId = dataFusionService.getDefaultContextId();
            if (defaultContextId == 0) {
                throw new RuntimeException("No default DataFusion context available");
            }

            // Execute the Substrait query plan using the existing default context
            String result = dataFusionService.executeSubstraitQueryPlan(defaultContextId, queryPlanIR);
            logger.info("Query execution result string:");
            logger.info(result);
            ObjectMapper mapper = new ObjectMapper();
            TypeReference<ArrayList<Map<String, Object>>> typeRef = new TypeReference<>() {};
            finalRes = mapper.readValue(result, typeRef);
            logger.info("Query execution result:");
            logger.info(finalRes);

//            long streamPtr = dataFusionService.executeSubstraitQueryStream(defaultContextId, queryPlanIR);
//            if (streamPtr != 0) {
//                try {
//                    // Get results batch by batch
//                    String batch;
//                    while ((batch = dataFusionService.getNextBatch(streamPtr)) != null) {
//                        // Process batch JSON...
//                        System.out.println(batch);
//                        ObjectMapper mapper = new ObjectMapper();
//                        TypeReference<ArrayList<HashMap<String, Object>>> typeRef = new TypeReference<>() {};
//                        ArrayList<HashMap<String, Object>> finalRes = mapper.readValue(batch, typeRef);
//                        logger.info("Query execution stream result:");
//                        logger.info(finalRes);
//                    }
//                } finally {
//                    dataFusionService.closeStream(streamPtr);
//                }
//            }

        } catch (Exception exception) {
            logger.error("Failed to execute Substrait query plan", exception);
        }
        return finalRes;
    }
}
