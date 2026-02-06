/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.rest;

import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.planner.action.QSearchAction;
import org.opensearch.planner.action.QSearchRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler for the /_qsearch endpoint.
 *
 * This endpoint accepts DSL queries and executes them through the query optimization
 * and physical planning pipeline.
 *
 * Example request:
 * POST /_qsearch
 * {
 *   "query": {
 *     "range": { "price": { "gt": 100 } }
 *   },
 *   "aggs": {
 *     "by_category": {
 *       "terms": { "field": "category" }
 *     }
 *   }
 * }
 */
public class RestQSearchAction extends BaseRestHandler {

    /**
     * Constructs a new RestQSearchAction.
     */
    public RestQSearchAction() {}

    @Override
    public String getName() {
        return "qsearch_action";
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_qsearch"));
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        // Create QSearchRequest
        QSearchRequest qSearchRequest = new QSearchRequest();

        // Parse index parameter (optional - defaults to all indices)
        String indexParam = request.param("index");
        if (indexParam != null) {
            qSearchRequest.indices(Strings.splitStringByCommaToArray(indexParam));
        }

        // Parse DSL JSON from request body
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            if (parser != null) {
                // Parse the DSL JSON into SearchSourceBuilder
                searchSourceBuilder.parseXContent(parser, true);
            }
        }

        qSearchRequest.source(searchSourceBuilder);

        // Execute the request through the transport layer
        return channel -> client.execute(QSearchAction.INSTANCE, qSearchRequest, new RestStatusToXContentListener<>(channel));
    }
}
