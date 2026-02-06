/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.planner.action.QSearchAction;
import org.opensearch.planner.action.QSearchRequest;
import org.opensearch.planner.action.QSearchResponse;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

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
     * Test 1: Action Registration
     * Verifies the action is registered and accepts basic match_all queries.
     */
    public void testQSearchEndpointExists() {
        QSearchRequest request = new QSearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder().query(new MatchAllQueryBuilder());
        request.source(source);

        QSearchResponse response = client().execute(QSearchAction.INSTANCE, request).actionGet();
        assertNotNull(response);
        assertEquals(200, response.status().getStatus());
    }

    /**
     * Test 2: Response Structure
     * Validates response fields (message, took time, status code).
     */
    public void testQSearchEndpointResponse() {
        QSearchRequest request = new QSearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder().query(new MatchAllQueryBuilder());
        request.source(source);

        QSearchResponse response = client().execute(QSearchAction.INSTANCE, request).actionGet();
        
        // Verify response structure
        assertNotNull("Response should have a message", response.getMessage());
        assertTrue("Response should have took time >= 0", response.getTookInMillis() >= 0);
        assertEquals("Response should have status 200", 200, response.status().getStatus());
    }

    /**
     * Test 3: Request Validation
     * Tests validation logic (empty source should fail).
     */
    public void testQSearchEndpointValidation() {
        QSearchRequest request = new QSearchRequest();
        // Empty source should fail validation

        ActionRequestValidationException validationException = request.validate();
        assertNotNull("Expected validation error for empty request body", validationException);
        assertTrue("Validation error should mention missing source", 
            validationException.getMessage().contains("search source is missing"));
    }

    /**
     * Test 4: Range Query Support
     * Tests range queries (e.g., price > 100).
     */
    public void testQSearchWithRangeQuery() {
        QSearchRequest request = new QSearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder()
            .query(new RangeQueryBuilder("price").gt(100));
        request.source(source);

        QSearchResponse response = client().execute(QSearchAction.INSTANCE, request).actionGet();
        assertNotNull(response);
        assertEquals(200, response.status().getStatus());
    }

    /**
     * Test 5: Aggregation Query Support
     * Tests aggregation queries (e.g., terms aggregation).
     */
    public void testQSearchWithAggregation() {
        QSearchRequest request = new QSearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder()
            .query(new MatchAllQueryBuilder())
            .aggregation(AggregationBuilders.terms("by_category").field("category"));
        request.source(source);

        QSearchResponse response = client().execute(QSearchAction.INSTANCE, request).actionGet();
        assertNotNull(response);
        assertEquals(200, response.status().getStatus());
    }
}
