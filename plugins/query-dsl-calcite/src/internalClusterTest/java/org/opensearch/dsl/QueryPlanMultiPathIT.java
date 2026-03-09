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
 * Integration tests for multi-entry QueryPlan conversion.
 *
 * Verifies that DSL queries produce the correct number of entries:
 * - Query only (no aggs): 1 entry → HITS
 * - Aggs with size=0: 1 entry → AGGREGATION
 * - Aggs with size>0: 2 entries → HITS + AGGREGATION
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
     * Expected: 1 entry → HITS
     */
    public void testQueryOnlyProducesSingleHitsEntry() throws Exception {
        createTestIndex();

        SearchSourceBuilder searchSource = new SearchSourceBuilder();
        searchSource.query(QueryBuilders.termQuery("category", "electronics"));

        QueryPlans plans = getConverterService().convert(searchSource, INDEX_NAME);

        assertEquals("Should have exactly 1 entry", 1, plans.getAll().size());
        assertTrue("Should have HITS entry", plans.has(QueryPlans.Type.HITS));
        assertFalse("Should NOT have AGGREGATION entry",
            plans.has(QueryPlans.Type.AGGREGATION));

        // Verify the HITS entry contains a LogicalFilter
        RelNode hitsRelNode = plans.get(QueryPlans.Type.HITS).get().relNode();
        String hitsExplain = hitsRelNode.explain();
        assertThat(hitsExplain, containsString("LogicalFilter"));
        assertThat(hitsExplain, containsString("LogicalTableScan"));
        // Should NOT contain aggregation
        assertThat(hitsExplain, not(containsString("LogicalAggregate")));
    }

    /**
     * Scenario: Aggregation with size=0 (aggregation-only query).
     * Expected: 1 entry → AGGREGATION
     */
    public void testAggWithSizeZeroProducesSingleAggEntry() throws Exception {
        createTestIndex();

        SearchSourceBuilder searchSource = new SearchSourceBuilder();
        searchSource.query(QueryBuilders.termQuery("category", "electronics"));
        searchSource.aggregation(
            AggregationBuilders.terms("by_brand")
                .field("brand")
                .subAggregation(AggregationBuilders.avg("avg_price").field("price"))
        );
        searchSource.size(0);

        QueryPlans plans = getConverterService().convert(searchSource, INDEX_NAME);

        assertEquals("Should have exactly 1 entry", 1, plans.getAll().size());
        assertFalse("Should NOT have HITS entry", plans.has(QueryPlans.Type.HITS));
        assertTrue("Should have AGGREGATION entry",
            plans.has(QueryPlans.Type.AGGREGATION));

        // Verify the AGGREGATION entry contains LogicalAggregate
        RelNode aggRelNode = plans.get(QueryPlans.Type.AGGREGATION).get().relNode();
        String aggExplain = aggRelNode.explain();
        assertThat(aggExplain, containsString("LogicalAggregate"));
        assertThat(aggExplain, containsString("LogicalFilter"));
        assertThat(aggExplain, containsString("LogicalTableScan"));
    }

    /**
     * Scenario: Aggregation with size > 0 (need both hits and aggs).
     * Expected: 2 entries → HITS + AGGREGATION
     */
    public void testAggWithSizeGreaterThanZeroProducesTwoEntries() throws Exception {
        createTestIndex();

        SearchSourceBuilder searchSource = new SearchSourceBuilder();
        searchSource.query(QueryBuilders.termQuery("category", "electronics"));
        searchSource.aggregation(
            AggregationBuilders.terms("by_brand")
                .field("brand")
                .subAggregation(AggregationBuilders.avg("avg_price").field("price"))
        );
        searchSource.size(10);

        QueryPlans plans = getConverterService().convert(searchSource, INDEX_NAME);

        assertEquals("Should have exactly 2 entries", 2, plans.getAll().size());
        assertTrue("Should have HITS entry", plans.has(QueryPlans.Type.HITS));
        assertTrue("Should have AGGREGATION entry",
            plans.has(QueryPlans.Type.AGGREGATION));

        // Verify the HITS entry does NOT contain aggregation
        RelNode hitsRelNode = plans.get(QueryPlans.Type.HITS).get().relNode();
        String hitsExplain = hitsRelNode.explain();
        assertThat(hitsExplain, containsString("LogicalFilter"));
        assertThat(hitsExplain, containsString("LogicalTableScan"));
        assertThat("HITS entry should NOT have LogicalAggregate",
            hitsExplain, not(containsString("LogicalAggregate")));

        // Verify the AGGREGATION entry contains aggregation
        RelNode aggRelNode = plans.get(QueryPlans.Type.AGGREGATION).get().relNode();
        String aggExplain = aggRelNode.explain();
        assertThat(aggExplain, containsString("LogicalAggregate"));
        assertThat(aggExplain, containsString("LogicalFilter"));
        assertThat(aggExplain, containsString("LogicalTableScan"));
    }

    /**
     * Scenario: Aggregation with default size (no explicit size set).
     * Default size is 10 (> 0), so should produce 2 entries.
     * Expected: 2 entries → HITS + AGGREGATION
     */
    public void testAggWithDefaultSizeProducesTwoEntries() throws Exception {
        createTestIndex();

        SearchSourceBuilder searchSource = new SearchSourceBuilder();
        searchSource.aggregation(
            AggregationBuilders.terms("by_brand")
                .field("brand")
        );
        // No explicit size — defaults to 10

        QueryPlans plans = getConverterService().convert(searchSource, INDEX_NAME);

        assertEquals("Should have exactly 2 entries", 2, plans.getAll().size());
        assertTrue("Should have HITS entry", plans.has(QueryPlans.Type.HITS));
        assertTrue("Should have AGGREGATION entry",
            plans.has(QueryPlans.Type.AGGREGATION));
    }

    /**
     * Scenario: No query, no aggregation (bare search).
     * Expected: 1 entry → HITS
     */
    public void testBareSearchProducesSingleHitsEntry() throws Exception {
        createTestIndex();

        SearchSourceBuilder searchSource = new SearchSourceBuilder();

        QueryPlans plans = getConverterService().convert(searchSource, INDEX_NAME);

        assertEquals("Should have exactly 1 entry", 1, plans.getAll().size());
        assertTrue("Should have HITS entry", plans.has(QueryPlans.Type.HITS));
        assertFalse("Should NOT have AGGREGATION entry",
            plans.has(QueryPlans.Type.AGGREGATION));
    }

    /**
     * Scenario: Aggregation with size > 0, verify both entries share the same filter.
     * Both HITS and AGGREGATION entries should contain the same filter condition.
     */
    public void testBothEntriesShareSameFilter() throws Exception {
        createTestIndex();

        SearchSourceBuilder searchSource = new SearchSourceBuilder();
        searchSource.query(QueryBuilders.termQuery("category", "electronics"));
        searchSource.aggregation(
            AggregationBuilders.avg("avg_price").field("price")
        );
        searchSource.size(5);

        QueryPlans plans = getConverterService().convert(searchSource, INDEX_NAME);

        assertEquals("Should have 2 entries", 2, plans.getAll().size());

        // Both entries should contain the same filter
        String hitsExplain = plans.get(QueryPlans.Type.HITS).get().relNode().explain();
        String aggExplain = plans.get(QueryPlans.Type.AGGREGATION).get().relNode().explain();

        assertThat(hitsExplain, containsString("'electronics')"));
        assertThat(aggExplain, containsString("'electronics')"));
    }

    /**
     * Scenario: Aggregation with size > 0 and pagination.
     * HITS entry should have offset/fetch, AGGREGATION entry should NOT.
     */
    public void testHitsEntryHasPaginationAggEntryDoesNot() throws Exception {
        createTestIndex();

        SearchSourceBuilder searchSource = new SearchSourceBuilder();
        searchSource.query(QueryBuilders.termQuery("category", "electronics"));
        searchSource.aggregation(
            AggregationBuilders.terms("by_brand").field("brand")
        );
        searchSource.from(10);
        searchSource.size(20);

        QueryPlans plans = getConverterService().convert(searchSource, INDEX_NAME);

        assertEquals("Should have 2 entries", 2, plans.getAll().size());

        // HITS entry should have pagination (LogicalSort with offset/fetch)
        String hitsExplain = plans.get(QueryPlans.Type.HITS).get().relNode().explain();
        assertThat("HITS entry should have offset", hitsExplain, containsString("offset"));
        assertThat("HITS entry should have fetch", hitsExplain, containsString("fetch"));

        // AGGREGATION entry should NOT have pagination
        String aggExplain = plans.get(QueryPlans.Type.AGGREGATION).get().relNode().explain();
        assertThat("AGG entry should NOT have offset", aggExplain, not(containsString("offset")));
        assertThat("AGG entry should NOT have fetch", aggExplain, not(containsString("fetch")));
    }

    /**
     * Scenario: Verify convertDsl still works end-to-end with multi-entry queries.
     * The full pipeline (convert → execute → build response) should not throw.
     */
    public void testConvertDslEndToEndWithMultiEntry() throws Exception {
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
