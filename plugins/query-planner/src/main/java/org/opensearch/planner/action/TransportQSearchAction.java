/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.action;

import org.apache.calcite.rel.RelNode;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.calcite.CalciteConverterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.planner.converter.ConversionException;
import org.opensearch.planner.converter.DslToCalciteService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.util.Locale;

/**
 * Transport action for executing optimized queries.
 *
 * This is the entry point for query execution through the optimization pipeline.
 *
 * Current implementation (Phase 1.3):
 * 1. Convert DSL to Calcite logical plan (via query-dsl-calcite plugin)
 *
 * Future phases will add:
 * 2. Optimize the logical plan using Calcite rules
 * 3. Generate physical plan with engine assignments
 * 4. Split plan into Lucene and DataFusion segments
 * 5. Execute segments and coordinate results
 */
public class TransportQSearchAction extends HandledTransportAction<QSearchRequest, QSearchResponse> {

    private final DslToCalciteService dslToCalciteService;

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
            } catch (ConversionException e) {
                listener.onFailure(e);
                return;
            }

            String planString = logicalPlan.explain();

            // Build response with separate fields
            String message = String.format(
                Locale.ROOT,
                "Successfully converted DSL query to Calcite logical plan for index: %s",
                indexName
            );

            long tookInMillis = System.currentTimeMillis() - startTime;
            QSearchResponse response = new QSearchResponse(message, planString, indexName, tookInMillis);

            listener.onResponse(response);

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
