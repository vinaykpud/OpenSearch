/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.dsl.queryplanner.QueryPlanExecutor;
import org.opensearch.dsl.result.QueryPlanResult;
import org.opensearch.dsl.result.SearchResponseBuilder;
import org.opensearch.tasks.Task;

/**
 * ActionFilter that intercepts search requests and re-routes them through the
 * DSL-to-Calcite conversion pipeline, bypassing TransportSearchAction entirely.
 *
 * This decouples the multi-engine DSL execution path from OpenSearch core.
 */
public class DslRerouteFilter implements ActionFilter {

    private static final Logger logger = LogManager.getLogger(DslRerouteFilter.class);

    private final DslLogicalPlanService converterService;
    private final QueryPlanExecutor queryPlanExecutor;

    public DslRerouteFilter(DslLogicalPlanService converterService, QueryPlanExecutor queryPlanExecutor) {
        this.converterService = converterService;
        this.queryPlanExecutor = queryPlanExecutor;
    }

    @Override
    public int order() {
        return Integer.MIN_VALUE;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (!shouldReroute(action, request)) {
            chain.proceed(task, action, request, listener);
            return;
        }

        SearchRequest searchRequest = (SearchRequest) request;
        String indexName = searchRequest.indices()[0];

        try {
            long startTime = System.currentTimeMillis();
            QueryPlans plans = converterService.convert(searchRequest.source(), indexName);
            QueryPlanResult result = queryPlanExecutor.execute(plans);
            long tookInMillis = System.currentTimeMillis() - startTime;

            SearchResponse response = SearchResponseBuilder.build(
                result, searchRequest.source(), converterService.getAggregationRegistry(), tookInMillis
            );
            ((ActionListener<SearchResponse>) listener).onResponse(response);
        } catch (Exception e) {
            logger.error("DSL conversion failed, request will not fall through to legacy path", e);
            listener.onFailure(e);
        }
    }

    private static boolean shouldReroute(String action, ActionRequest request) {
        if (!SearchAction.NAME.equals(action)) {
            return false;
        }
        if (!(request instanceof SearchRequest searchRequest)) {
            return false;
        }
        if (searchRequest.source() == null) {
            return false;
        }
        if (searchRequest.indices() == null || searchRequest.indices().length == 0) {
            return false;
        }
        return true;
    }
}
