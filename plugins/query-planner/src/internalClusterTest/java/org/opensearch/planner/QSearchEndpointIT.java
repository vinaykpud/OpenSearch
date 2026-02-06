/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Integration tests for the /_qsearch REST endpoint.
 *
 * These tests verify that the endpoint is properly registered and can accept requests.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class QSearchEndpointIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Load only the query-planner plugin for basic endpoint testing
        return Collections.singletonList(QueryPlannerPlugin.class);
    }

    /**
     * Test that the /_qsearch endpoint is registered and accepts POST requests.
     */
    public void testQSearchEndpointExists() throws IOException {
        Request request = new Request("POST", "/_qsearch");
        request.setJsonEntity("{\"query\":{\"match_all\":{}}}");

        Response response = getRestClient().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    /**
     * Test that the /_qsearch endpoint returns a valid response structure.
     */
    public void testQSearchEndpointResponse() throws IOException {
        Request request = new Request("POST", "/_qsearch");
        request.setJsonEntity("{\"query\":{\"match_all\":{}}}");

        Response response = getRestClient().performRequest(request);
        String responseBody = response.getEntity().getContent().toString();

        // Verify response contains expected fields
        assertTrue("Response should contain 'message' field", responseBody.contains("message"));
        assertTrue("Response should contain 'took' field", responseBody.contains("took"));
    }

    /**
     * Test that the /_qsearch endpoint validates request body.
     */
    public void testQSearchEndpointValidation() {
        Request request = new Request("POST", "/_qsearch");
        // Empty body should fail validation

        try {
            getRestClient().performRequest(request);
            fail("Expected validation error for empty request body");
        } catch (ResponseException e) {
            // Expected - validation should fail
            assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        } catch (IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    /**
     * Test that the /_qsearch endpoint accepts range queries.
     */
    public void testQSearchWithRangeQuery() throws IOException {
        Request request = new Request("POST", "/_qsearch");
        request.setJsonEntity("{\"query\":{\"range\":{\"price\":{\"gt\":100}}}}");

        Response response = getRestClient().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseBody = response.getEntity().getContent().toString();
        assertTrue("Response should contain 'message' field", responseBody.contains("message"));
    }

    /**
     * Test that the /_qsearch endpoint accepts aggregation queries.
     */
    public void testQSearchWithAggregation() throws IOException {
        Request request = new Request("POST", "/_qsearch");
        request.setJsonEntity("{\"query\":{\"match_all\":{}},\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\"}}}}");

        Response response = getRestClient().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseBody = response.getEntity().getContent().toString();
        assertTrue("Response should contain 'message' field", responseBody.contains("message"));
    }
}
