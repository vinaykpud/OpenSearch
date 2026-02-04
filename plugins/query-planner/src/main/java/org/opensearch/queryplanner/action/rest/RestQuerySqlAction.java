/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.action.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.queryplanner.action.QuerySqlAction;
import org.opensearch.queryplanner.action.QuerySqlRequest;
import org.opensearch.queryplanner.action.QuerySqlResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler for executing SQL queries via the Query Planner.
 *
 * <p>This endpoint accepts SQL queries, parses them using Calcite, optimizes
 * the query plan, and executes via the distributed query executor.
 *
 * <h2>Endpoints:</h2>
 * <ul>
 *   <li>POST /_sql - Execute a SQL query</li>
 *   <li>POST /_query_sql - Alias for /_sql</li>
 * </ul>
 *
 * <h2>Request body:</h2>
 * <pre>{@code
 * {
 *   "query": "SELECT * FROM my_index WHERE status = 'active'"
 * }
 * }</pre>
 *
 * <h2>Response:</h2>
 * <pre>{@code
 * {
 *   "took": 123,
 *   "_shards": { "total": 5, "successful": 5, "failed": 0 },
 *   "columns": ["col1", "col2"],
 *   "rows": [[val1, val2], [val3, val4]]
 * }
 * }</pre>
 *
 * <h2>Execution Flow:</h2>
 * <ol>
 *   <li>Parse SQL with Calcite (via CalciteSqlParser)</li>
 *   <li>Optimize plan (via OptimizerPipeline)</li>
 *   <li>Execute distributed (via TransportQueryPlanAction)</li>
 *   <li>Merge results and return</li>
 * </ol>
 */
public class RestQuerySqlAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "query_sql_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_sql"),
            new Route(POST, "/_query_sql")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Parse request body
        Map<String, Object> body = request.contentParser().map();

        // Get SQL query
        Object queryObj = body.get("query");
        if (queryObj == null) {
            throw new IllegalArgumentException("'query' is required");
        }

        String sql = queryObj.toString();
        if (sql.trim().isEmpty()) {
            throw new IllegalArgumentException("'query' cannot be empty");
        }

        // Create request
        QuerySqlRequest sqlRequest = new QuerySqlRequest(sql);

        return channel -> client.execute(
            QuerySqlAction.INSTANCE,
            sqlRequest,
            new RestResponseListener<QuerySqlResponse>(channel) {
                @Override
                public RestResponse buildResponse(QuerySqlResponse response) throws Exception {
                    return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), request));
                }
            }
        );
    }
}
