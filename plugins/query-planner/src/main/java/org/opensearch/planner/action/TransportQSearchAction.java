/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.Locale;

/**
 * Transport action for executing optimized queries.
 *
 * This is the entry point for query execution through the optimization pipeline.
 * Currently returns a placeholder response. Will be enhanced in later phases to:
 * 1. Convert DSL to Calcite logical plan (via query-dsl-calcite plugin)
 * 2. Optimize the logical plan using Calcite rules
 * 3. Generate physical plan with engine assignments
 * 4. Split plan into Lucene and DataFusion segments
 * 5. Execute segments and coordinate results
 */
public class TransportQSearchAction extends HandledTransportAction<QSearchRequest, QSearchResponse> {

    /**
     * Constructs a new TransportQSearchAction.
     *
     * @param transportService the transport service
     * @param actionFilters the action filters
     */
    @Inject
    public TransportQSearchAction(TransportService transportService, ActionFilters actionFilters) {
        super(QSearchAction.NAME, transportService, actionFilters, QSearchRequest::new);
    }

    @Override
    protected void doExecute(Task task, QSearchRequest request, ActionListener<QSearchResponse> listener) {
        // TODO: Implement query optimization and execution pipeline in later phases
        // For now, return a placeholder response indicating the endpoint is working

        long startTime = System.currentTimeMillis();

        try {
            // Placeholder: Just acknowledge receipt of the query
            String message = String.format(
                Locale.ROOT,
                "Query planner received request for indices: %s with source: %s",
                request.indices() != null && request.indices().length > 0 ? String.join(", ", request.indices()) : "all",
                request.source() != null ? "present" : "missing"
            );

            long tookInMillis = System.currentTimeMillis() - startTime;
            QSearchResponse response = new QSearchResponse(message, tookInMillis);

            listener.onResponse(response);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
