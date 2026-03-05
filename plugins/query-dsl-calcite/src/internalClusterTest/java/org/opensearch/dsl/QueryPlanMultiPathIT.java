/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.apache.calcite.rel.RelNode;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * Integration tests for multi-path QueryPlan conversion.
 *
 * Verifies that DSL queries produce the correct number of execution paths:
 * - Query only (no aggs): 1 path → HITS
 * - Aggs with size=0: 1 path → FILTER_AGGREGATION
 * - Aggs with size>0: 2 paths → HITS + FILTER_AGGREGATION
 */
public class QueryPlanMultiPathIT extends DslLogicalPlanIntegrationTestBase {

    private static final String INDEX_NAME = "test-multi-path";
    private static final String MAPPING = "{"
        + "\"properties\": {"
        + "  \"category\": {\"type\": \"keyword\"},"
        + "  \"brand\": {\"type\": \"keyword\"},"
        + "  \"price\": {\"type\": \"long\"}"
        + "}"
        + "}";

    private void createTestIndex() {
        client().admin().indices().prepareCreate(INDEX_NAME)
            .setMapping(MAPPING)
            .get();
        ensureGreen(INDEX_NAME);
    }

    private DslLogicalPlanService getConverterService() {
        DslLogicalPlanPlugin plugin = getPlugin(DslLogicalPlanPlugin.class);
        return plugin.getConverterService();
    }

    /**
     * Scenario: Query only, no aggregations.
     * Expected: 1 path → HITS
     */
    public void testQueryOnlyProducesSingleHitsPath() throws Exception {
        createTestIndex();

        SearchSourceBuilder searchSource = new SearchSourceBuilder();
        searchSource.query(QueryBuilders.termQuery("category", "electronics"));

        QueryPlan plan = getConverterService().convert(searchSource, INDEX_NAME);

        assertEquals("Should have exactly 1 path", 1, plan.getAllPaths().size());
        assertTrue("Should have HITS path", plan.hasPath(ExecutionPath.PathRole.HITS));
        assertFalse("Should NOT have FILTER_AGGREGATION path",
            plan.hasPath(ExecutionPath.PathRole.FILTER_AGGREGATION));

        // Verify the HITS path contains a LogicalFilter
        RelNode hitsRelNode = plan.getPath(ExecutionPath.PathRole.HITS).get().getRelNode();
        String hitsExplain = hitsRelNode.explain();
        assertThat(hitsExplain, containsString("LogicalFilter"));
        assertThat(hitsExplain, containsString("LogicalTableScan"));
        // Should NOT contain aggregation
        assertThat(hitsExplain, not(containsString("LogicalAggregate")));
    }

    /**
     * Scenario: Aggregation with size=0 (aggregation-only query).
     * Expected: 1 path → FILTER_AGGREGATION
     */
    public void testAggWithSizeZeroProducesSingleAggPath() throws Exception {
        createTestIndex();

        SearchSourceBuilder searchSource = new SearchSourceBuilder();
        searchSource.query(QueryBuilders.termQuery("category", "electronics"));
        searchSource.aggregation(
            AggregationBuilders.terms("by_brand")
                .field("brand")
                .subAggregation(AggregationBuilders.avg("avg_price").field("price"))
        );
        searchSource.size(0);

        QueryPlan plan = getConverterService().convert(searchSource, INDEX_NAME);

        assertEquals("Should have exactly 1 path", 1, plan.getAllPaths().size());
        assertFalse("Should NOT have HITS path", plan.hasPath(ExecutionPath.PathRole.HITS));
        assertTrue("Should have FILTER_AGGREGATION path",
            plan.hasPath(ExecutionPath.PathRole.FILTER_AGGREGATION));

        // Verify the FILTER_AGGREGATION path contains LogicalAggregate
        RelNode aggRelNode = plan.getPath(ExecutionPath.PathRole.FILTER_AGGREGATION).get().getRelNode();
        String aggExplain = aggRelNode.explain();
        assertThat(aggExplain, containsString("LogicalAggregate"));
        assertThat(aggExplain, containsString("LogicalFilter"));
        assertThat(aggExplain, containsString("LogicalTableScan"));
    }

    /**
     * Scenario: Aggregation with size > 0 (need both hits and aggs).
     * Expected: 2 paths → HITS + FILTER_AGGREGATION
     */
    public void testAggWithSizeGreaterThanZeroProducesTwoPaths() throws Exception {
        createTestIndex();

        SearchSourceBuilder searchSource = new SearchSourceBuilder();
        searchSource.query(QueryBuilders.termQuery("category", "electronics"));
        searchSource.aggregation(
            AggregationBuilders.terms("by_brand")
                .field("brand")
                .subAggregation(AggregationBuilders.avg("avg_price").field("price"))
        );
        searchSource.size(10);

        QueryPlan plan = getConverterService().convert(searchSource, INDEX_NAME);

        assertEquals("Should have exactly 2 paths", 2, plan.getAllPaths().size());
        assertTrue("Should have HITS path", plan.hasPath(ExecutionPath.PathRole.HITS));
        assertTrue("Should have FILTER_AGGREGATION path",
            plan.hasPath(ExecutionPath.PathRole.FILTER_AGGREGATION));

        // Verify the HITS path does NOT contain aggregation
        RelNode hitsRelNode = plan.getPath(ExecutionPath.PathRole.HITS).get().getRelNode();
        String hitsExplain = hitsRelNode.explain();
        assertThat(hitsExplain, containsString("LogicalFilter"));
        assertThat(hitsExplain, containsString("LogicalTableScan"));
        assertThat("HITS path should NOT have LogicalAggregate",
            hitsExplain, not(containsString("LogicalAggregate")));

        // Verify the FILTER_AGGREGATION path contains aggregation
        RelNode aggRelNode = plan.getPath(ExecutionPath.PathRole.FILTER_AGGREGATION).get().getRelNode();
        String aggExplain = aggRelNode.explain();
        assertThat(aggExplain, containsString("LogicalAggregate"));
        assertThat(aggExplain, containsString("LogicalFilter"));
        assertThat(aggExplain, containsString("LogicalTableScan"));
    }

    /**
     * Scenario: Aggregation with default size (no explicit size set).
     * Default size is 10 (> 0), so should produce 2 paths.
     * Expected: 2 paths → HITS + FILTER_AGGREGATION
     */
    public void testAggWithDefaultSizeProducesTwoPaths() throws Exception {
        createTestIndex();

        SearchSourceBuilder searchSource = new SearchSourceBuilder();
        searchSource.aggregation(
            AggregationBuilders.terms("by_brand")
                .field("brand")
        );
        // No explicit size — defaults to 10

        QueryPlan plan = getConverterService().convert(searchSource, INDEX_NAME);

        assertEquals("Should have exactly 2 paths", 2, plan.getAllPaths().size());
        assertTrue("Should have HITS path", plan.hasPath(ExecutionPath.PathRole.HITS));
        assertTrue("Should have FILTER_AGGREGATION path",
            plan.hasPath(ExecutionPath.PathRole.FILTER_AGGREGATION));
    }

    /**
     * Scenario: No query, no aggregation (bare search).
     * Expected: 1 path → HITS
     */
    public void testBareSearchProducesSingleHitsPath() throws Exception {
        createTestIndex();

        SearchSourceBuilder searchSource = new SearchSourceBuilder();

        QueryPlan plan = getConverterService().convert(searchSource, INDEX_NAME);

        assertEquals("Should have exactly 1 path", 1, plan.getAllPaths().size());
        assertTrue("Should have HITS path", plan.hasPath(ExecutionPath.PathRole.HITS));
        assertFalse("Should NOT have FILTER_AGGREGATION path",
            plan.hasPath(ExecutionPath.PathRole.FILTER_AGGREGATION));
    }

    /**
     * Scenario: Aggregation with size > 0, verify both paths share the same filter.
     * Both HITS and FILTER_AGGREGATION paths should contain the same filter condition.
     */
    public void testBothPathsShareSameFilter() throws Exception {
        createTestIndex();

        SearchSourceBuilder searchSource = new SearchSourceBuilder();
        searchSource.query(QueryBuilders.termQuery("category", "electronics"));
        searchSource.aggregation(
            AggregationBuilders.avg("avg_price").field("price")
        );
        searchSource.size(5);

        QueryPlan plan = getConverterService().convert(searchSource, INDEX_NAME);

        assertEquals("Should have 2 paths", 2, plan.getAllPaths().size());

        // Both paths should contain the same filter
        String hitsExplain = plan.getPath(ExecutionPath.PathRole.HITS).get().getRelNode().explain();
        String aggExplain = plan.getPath(ExecutionPath.PathRole.FILTER_AGGREGATION).get().getRelNode().explain();

        assertThat(hitsExplain, containsString("'electronics')"));
        assertThat(aggExplain, containsString("'electronics')"));
    }

    /**
     * Scenario: Aggregation with size > 0 and pagination.
     * HITS path should have offset/fetch, FILTER_AGGREGATION path should NOT.
     */
    public void testHitsPathHasPaginationAggPathDoesNot() throws Exception {
        createTestIndex();

        SearchSourceBuilder searchSource = new SearchSourceBuilder();
        searchSource.query(QueryBuilders.termQuery("category", "electronics"));
        searchSource.aggregation(
            AggregationBuilders.terms("by_brand").field("brand")
        );
        searchSource.from(10);
        searchSource.size(20);

        QueryPlan plan = getConverterService().convert(searchSource, INDEX_NAME);

        assertEquals("Should have 2 paths", 2, plan.getAllPaths().size());

        // HITS path should have pagination (LogicalSort with offset/fetch)
        String hitsExplain = plan.getPath(ExecutionPath.PathRole.HITS).get().getRelNode().explain();
        assertThat("HITS path should have offset", hitsExplain, containsString("offset"));
        assertThat("HITS path should have fetch", hitsExplain, containsString("fetch"));

        // FILTER_AGGREGATION path should NOT have pagination
        String aggExplain = plan.getPath(ExecutionPath.PathRole.FILTER_AGGREGATION).get().getRelNode().explain();
        assertThat("AGG path should NOT have offset", aggExplain, not(containsString("offset")));
        assertThat("AGG path should NOT have fetch", aggExplain, not(containsString("fetch")));
    }

    /**
     * Scenario: Verify convertDsl still works end-to-end with multi-path queries.
     * The full pipeline (convert → execute → build response) should not throw.
     */
    public void testConvertDslEndToEndWithMultiPath() throws Exception {
        createTestIndex();

        SearchSourceBuilder searchSource = new SearchSourceBuilder();
        searchSource.query(QueryBuilders.termQuery("category", "electronics"));
        searchSource.aggregation(
            AggregationBuilders.terms("by_brand")
                .field("brand")
                .subAggregation(AggregationBuilders.avg("avg_price").field("price"))
        );
        searchSource.size(10);

        DslLogicalPlanPlugin plugin = getPlugin(DslLogicalPlanPlugin.class);
        assertNotNull("convertDsl should return non-null response",
            plugin.convertDsl(searchSource, INDEX_NAME));
    }
}
