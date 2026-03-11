/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.dsl.QueryPlans;
import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.InternalSearchResponse;

import java.util.List;

/**
 * Converts execution results into OpenSearch {@link SearchResponse}.
 *
 * Merges HITS and multiple AGGREGATION execution results (one per granularity)
 * into a single SearchResponse by walking the original aggregation tree.
 */
public final class SearchResponseBuilder {

    private SearchResponseBuilder() {}

    /**
     * Builds a {@link SearchResponse} from execution results.
     *
     * @param planResult   the results from executing all query plans
     * @param searchSource the original search source (for aggregation tree structure)
     * @param aggRegistry  the aggregation registry (for finding handlers)
     * @param tookInMillis elapsed time in milliseconds
     * @return the constructed search response
     */
    public static SearchResponse build(QueryPlanResult planResult, SearchSourceBuilder searchSource,
            AggregationRegistry aggRegistry, long tookInMillis) throws ConversionException {

        SearchHits searchHits = buildHits(planResult);
        InternalAggregations aggs = buildAggregations(planResult, searchSource, aggRegistry);

        InternalSearchResponse internal = new InternalSearchResponse(
            searchHits, aggs, null, null, false, null, 1);
        return new SearchResponse(
            internal, null, 1, 1, 0, tookInMillis,
            ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    private static SearchHits buildHits(QueryPlanResult planResult) {
        return planResult.getResult(QueryPlans.Type.HITS)
            .map(HitsResponseBuilder::build)
            .orElse(new SearchHits(
                new org.opensearch.search.SearchHit[0],
                new TotalHits(0, TotalHits.Relation.EQUAL_TO),
                Float.NaN));
    }

    private static InternalAggregations buildAggregations(QueryPlanResult planResult,
            SearchSourceBuilder searchSource, AggregationRegistry aggRegistry) throws ConversionException {

        List<ExecutionResult> aggResults = planResult.getResults(QueryPlans.Type.AGGREGATION);
        if (aggResults.isEmpty()) {
            return null;
        }

        AggregationResponseBuilder builder = new AggregationResponseBuilder(aggRegistry, aggResults);
        return builder.build(searchSource.aggregations().getAggregatorFactories());
    }
}
