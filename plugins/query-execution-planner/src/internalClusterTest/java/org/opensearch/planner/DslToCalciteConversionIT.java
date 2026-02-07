/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.calcite.DslCalcitePlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.planner.action.QSearchRequest;
import org.opensearch.planner.action.QSearchResponse;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;

import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.rangeQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;

/**
 * Integration tests for DSL to Calcite conversion.
 *
 * These tests verify that the query-planner plugin correctly integrates with
 * the query-dsl-calcite plugin to convert OpenSearch DSL queries to Calcite
 * logical plans.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class DslToCalciteConversionIT extends OpenSearchIntegTestCase {

    private static final String TEST_INDEX = "products";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Load both query-dsl-calcite and query-planner plugins
        // query-planner depends on CalciteConverterService from query-dsl-calcite
        return Arrays.asList(DslCalcitePlugin.class, QueryPlannerPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).build();
    }

    /**
     * Sets up test data before each test.
     */
    public void setUp() throws Exception {
        super.setUp();
        createTestIndex();
        indexTestData();
    }

    /**
     * Creates the test index with appropriate mappings.
     */
    private void createTestIndex() {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(TEST_INDEX);
        createIndexRequest.mapping(
            "{\n"
                + "  \"properties\": {\n"
                + "    \"name\": { \"type\": \"text\", \"fields\": { \"keyword\": { \"type\": \"keyword\" } } },\n"
                + "    \"category\": { \"type\": \"keyword\" },\n"
                + "    \"price\": { \"type\": \"long\" },\n"
                + "    \"quantity\": { \"type\": \"integer\" },\n"
                + "    \"in_stock\": { \"type\": \"boolean\" }\n"
                + "  }\n"
                + "}",
            XContentType.JSON
        );
        client().admin().indices().create(createIndexRequest).actionGet();
    }

    /**
     * Indexes test data into the test index.
     */
    private void indexTestData() {
        BulkRequest bulkRequest = new BulkRequest();

        // Add test documents
        bulkRequest.add(
            new IndexRequest(TEST_INDEX).source(
                "name",
                "Laptop",
                "category",
                "electronics",
                "price",
                1200,
                "quantity",
                10,
                "in_stock",
                true
            )
        );

        bulkRequest.add(
            new IndexRequest(TEST_INDEX).source("name", "Mouse", "category", "electronics", "price", 25, "quantity", 100, "in_stock", true)
        );

        bulkRequest.add(
            new IndexRequest(TEST_INDEX).source("name", "Book", "category", "books", "price", 15, "quantity", 50, "in_stock", true)
        );

        bulkRequest.add(
            new IndexRequest(TEST_INDEX).source("name", "Desk", "category", "furniture", "price", 300, "quantity", 5, "in_stock", false)
        );

        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse("Bulk indexing should not have failures", bulkResponse.hasFailures());

        // Refresh to make documents searchable
        client().admin().indices().prepareRefresh(TEST_INDEX).get();
    }

    /**
     * Tests conversion of a simple term query.
     */
    public void testTermQueryConversion() {
        SearchSourceBuilder searchSource = new SearchSourceBuilder().query(termQuery("category", "electronics"));

        QSearchRequest request = new QSearchRequest().indices(TEST_INDEX).source(searchSource);
        QSearchResponse response = client().execute(org.opensearch.planner.action.QSearchAction.INSTANCE, request).actionGet();

        assertNotNull("Response should not be null", response);
        assertNotNull("Response message should not be null", response.getMessage());
        assertNotNull("Response logical plan should not be null", response.getLogicalPlan());
        assertNotNull("Response index name should not be null", response.getIndexName());
        assertEquals("Index name should match", TEST_INDEX, response.getIndexName());
        assertTrue("Logical plan should contain LogicalFilter", response.getLogicalPlan().contains("LogicalFilter"));
        assertTrue("Logical plan should contain LogicalTableScan", response.getLogicalPlan().contains("LogicalTableScan"));
    }

    /**
     * Tests conversion of a range query.
     */
    public void testRangeQueryConversion() {
        SearchSourceBuilder searchSource = new SearchSourceBuilder().query(rangeQuery("price").gt(100));

        QSearchRequest request = new QSearchRequest().indices(TEST_INDEX).source(searchSource);
        QSearchResponse response = client().execute(org.opensearch.planner.action.QSearchAction.INSTANCE, request).actionGet();

        assertNotNull("Response should not be null", response);
        assertNotNull("Response message should not be null", response.getMessage());
        assertNotNull("Response logical plan should not be null", response.getLogicalPlan());
        assertTrue("Logical plan should contain LogicalFilter", response.getLogicalPlan().contains("LogicalFilter"));
        // The logical plan may use different casing or representation for field names
        // Just verify it contains a filter operation
    }

    /**
     * Tests conversion of a boolean query with multiple conditions.
     */
    public void testBoolQueryConversion() {
        SearchSourceBuilder searchSource = new SearchSourceBuilder().query(
            boolQuery().must(termQuery("category", "electronics")).filter(rangeQuery("price").gte(20).lte(1500))
        );

        QSearchRequest request = new QSearchRequest().indices(TEST_INDEX).source(searchSource);
        QSearchResponse response = client().execute(org.opensearch.planner.action.QSearchAction.INSTANCE, request).actionGet();

        assertNotNull("Response should not be null", response);
        assertNotNull("Response message should not be null", response.getMessage());
        assertNotNull("Response logical plan should not be null", response.getLogicalPlan());
        assertTrue("Logical plan should contain LogicalFilter", response.getLogicalPlan().contains("LogicalFilter"));
    }

    /**
     * Tests error handling when query source is missing.
     */
    public void testMissingQuerySource() {
        QSearchRequest request = new QSearchRequest().indices(TEST_INDEX);

        try {
            client().execute(org.opensearch.planner.action.QSearchAction.INSTANCE, request).actionGet();
            fail("Should have thrown exception for missing query source");
        } catch (Exception e) {
            // The exception message should mention "source" (from "Query source is required")
            String message = e.getMessage().toLowerCase(Locale.ROOT);
            assertTrue("Exception should mention source, but got: " + e.getMessage(), message.contains("source"));
        }
    }

    /**
     * Tests error handling when index is missing.
     */
    public void testMissingIndex() {
        SearchSourceBuilder searchSource = new SearchSourceBuilder().query(termQuery("category", "electronics"));

        QSearchRequest request = new QSearchRequest().source(searchSource);

        try {
            client().execute(org.opensearch.planner.action.QSearchAction.INSTANCE, request).actionGet();
            fail("Should have thrown exception for missing index");
        } catch (Exception e) {
            assertTrue("Exception should mention index", e.getMessage().toLowerCase(Locale.ROOT).contains("index"));
        }
    }
}
