/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.action;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.calcite.CalciteConverterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.planner.converter.ConversionException;
import org.opensearch.planner.converter.DslToCalciteService;
import org.opensearch.planner.optimizer.CalciteQueryOptimizer;
import org.opensearch.planner.optimizer.OptimizationException;
import org.opensearch.planner.optimizer.QueryOptimizer;
import org.opensearch.planner.physical.DefaultPhysicalPlanner;
import org.opensearch.planner.physical.PhysicalPlan;
import org.opensearch.planner.physical.PhysicalPlanner;
import org.opensearch.planner.physical.PlanningContext;
import org.opensearch.planner.physical.PlanningException;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.util.Locale;

/**
 * Transport action for executing optimized queries.
 *
 * This is the entry point for query execution through the optimization pipeline.
 *
 * Current implementation (Phase 2.3):
 * 1. Convert DSL to Calcite logical plan (via query-dsl-calcite plugin)
 * 2. Optimize the logical plan using Calcite rules (HepPlanner)
 * 3. Generate physical plan with engine assignments (Lucene/DataFusion)
 *
 * Future phases will add:
 * 4. Split plan into Lucene and DataFusion segments
 * 5. Execute segments and coordinate results
 */
public class TransportQSearchAction extends HandledTransportAction<QSearchRequest, QSearchResponse> {

    private static final Logger logger = LogManager.getLogger(TransportQSearchAction.class);

    private final DslToCalciteService dslToCalciteService;
    private final QueryOptimizer queryOptimizer;
    private final PhysicalPlanner physicalPlanner;

    /**
     * Constructs a new TransportQSearchAction.
     *
     * @param transportService the transport service
     * @param actionFilters the action filters
     * @param client the OpenSearch client
     * @param calciteConverterService the CalciteConverterService from query-dsl-calcite plugin
     */
    @Inject
    public TransportQSearchAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        CalciteConverterService calciteConverterService
    ) {
        super(QSearchAction.NAME, transportService, actionFilters, QSearchRequest::new);
        this.dslToCalciteService = new DslToCalciteService(client, calciteConverterService);
        this.queryOptimizer = new CalciteQueryOptimizer();
        this.physicalPlanner = new DefaultPhysicalPlanner();
    }

    @Override
    protected void doExecute(Task task, QSearchRequest request, ActionListener<QSearchResponse> listener) {
        long startTime = System.currentTimeMillis();

        try {
            // Validate request
            if (request.source() == null) {
                listener.onFailure(new IllegalArgumentException("Query source is required"));
                return;
            }

            if (request.indices() == null || request.indices().length == 0) {
                listener.onFailure(new IllegalArgumentException("At least one index is required"));
                return;
            }

            // For now, use the first index
            String indexName = request.indices()[0];

            // Convert DSL to Calcite logical plan
            RelNode logicalPlan;
            try {
                logicalPlan = dslToCalciteService.convertToLogicalPlan(request.source(), indexName);
                
                // Log the original logical plan
                logger.info("\n========================================");
                logger.info("LOGICAL PLAN (Before Optimization):");
                logger.info("========================================");
                logger.info("\n{}\n", logicalPlan.explain());
                
            } catch (ConversionException e) {
                listener.onFailure(e);
                return;
            }

            // Optimize the logical plan using Calcite rules
            RelNode optimizedPlan;
            try {
                optimizedPlan = queryOptimizer.optimize(logicalPlan);
                
                // Log the optimized logical plan
                logger.info("\n========================================");
                logger.info("LOGICAL PLAN (After Optimization):");
                logger.info("========================================");
                logger.info("\n{}\n", optimizedPlan.explain());
                
            } catch (OptimizationException e) {
                listener.onFailure(e);
                return;
            }

            // Get logical plan string for response
            String logicalPlanString = optimizedPlan.explain();

            // Generate physical plan with engine assignments
            PhysicalPlan physicalPlan;
            try {
                PlanningContext planningContext = PlanningContext.builder()
                    .withMetadata("indexName", indexName)
                    .build();
                physicalPlan = physicalPlanner.generatePhysicalPlan(optimizedPlan, planningContext);
            } catch (PlanningException e) {
                listener.onFailure(e);
                return;
            }

            // Serialize physical plan to JSON for response
            String physicalPlanJson;
            try {
                physicalPlanJson = physicalPlan.toJson();
                
                // Log the physical plan in a readable format
                logger.info("\n========================================");
                logger.info("PHYSICAL PLAN (with Engine Assignments):");
                logger.info("========================================");
                logger.info("\n{}\n", formatJsonForLogging(physicalPlanJson));
                
            } catch (Exception e) {
                listener.onFailure(new RuntimeException("Failed to serialize physical plan to JSON", e));
                return;
            }

            // Build response with both logical and physical plans
            String message = String.format(
                Locale.ROOT,
                "Successfully converted DSL query to logical and physical plans for index: %s",
                indexName
            );

            long tookInMillis = System.currentTimeMillis() - startTime;
            QSearchResponse response = new QSearchResponse(message, logicalPlanString, physicalPlanJson, indexName, tookInMillis);

            listener.onResponse(response);

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Formats JSON string for readable logging output.
     * Adds proper indentation to make the physical plan easier to read in logs.
     *
     * @param json the JSON string to format
     * @return formatted JSON string with indentation
     */
    private String formatJsonForLogging(String json) {
        try {
            // Simple JSON formatting with indentation
            StringBuilder formatted = new StringBuilder();
            int indent = 0;
            boolean inQuotes = false;
            
            for (int i = 0; i < json.length(); i++) {
                char c = json.charAt(i);
                
                if (c == '"' && (i == 0 || json.charAt(i - 1) != '\\')) {
                    inQuotes = !inQuotes;
                    formatted.append(c);
                } else if (!inQuotes) {
                    switch (c) {
                        case '{':
                        case '[':
                            formatted.append(c).append('\n');
                            indent++;
                            formatted.append("  ".repeat(indent));
                            break;
                        case '}':
                        case ']':
                            formatted.append('\n');
                            indent--;
                            formatted.append("  ".repeat(indent));
                            formatted.append(c);
                            break;
                        case ',':
                            formatted.append(c).append('\n');
                            formatted.append("  ".repeat(indent));
                            break;
                        case ':':
                            formatted.append(c).append(' ');
                            break;
                        default:
                            if (!Character.isWhitespace(c)) {
                                formatted.append(c);
                            }
                            break;
                    }
                } else {
                    formatted.append(c);
                }
            }
            
            return formatted.toString();
        } catch (Exception e) {
            // If formatting fails, return original JSON
            logger.warn("Failed to format JSON for logging", e);
            return json;
        }
    }
}
