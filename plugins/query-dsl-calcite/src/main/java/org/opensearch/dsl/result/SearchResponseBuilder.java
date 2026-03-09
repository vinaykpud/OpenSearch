/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.dsl.QueryPlans;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.internal.InternalSearchResponse;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts execution results into OpenSearch {@link SearchResponse}.
 *
 * Builds role-specific dummy data for each query plan present in the
 * {@link QueryPlanResult}: dummy hits for the HITS plan and dummy aggregations
 * for the AGGREGATION plan.
 */
public final class SearchResponseBuilder {

    private SearchResponseBuilder() {}

    /**
     * Builds a {@link SearchResponse} from the given query plan result.
     * Produces dummy hits if a HITS entry is present and dummy aggregations
     * if an AGGREGATION entry is present.
     *
     * @param planResult    the results from executing a query plan
     * @param tookInMillis  elapsed time in milliseconds
     * @return the constructed search response
     */
    public static SearchResponse build(QueryPlanResult planResult, long tookInMillis) {
        SearchHit[] hits = new SearchHit[0];
        InternalAggregations aggs = null;

        if (planResult.getResult(QueryPlans.Type.HITS).isPresent()) {
            hits = buildDummyHits();
        }

        if (planResult.getResult(QueryPlans.Type.AGGREGATION).isPresent()) {
            aggs = buildDummyAggregations();
        }

        return wrapInResponse(hits, aggs, tookInMillis);
    }

    /**
     * Dummy hits simulating: term(status=active) returning 3 documents.
     */
    private static SearchHit[] buildDummyHits() {
        return new SearchHit[] {
            createDummyHit(0, "Apple", 999.99),
            createDummyHit(1, "Apple", 1299.00),
            createDummyHit(2, "Samsung", 799.50),
        };
    }

    /**
     * Dummy aggregations simulating: terms(brand) with avg(price) sub-agg.
     *   Apple:   2 docs, avg_price = 1149.50
     *   Samsung: 1 doc,  avg_price = 799.50
     */
    private static InternalAggregations buildDummyAggregations() {
        InternalAvg appleAvg = new InternalAvg("avg_price", 2299.0, 2, DocValueFormat.RAW, Map.of());
        InternalAvg samsungAvg = new InternalAvg("avg_price", 799.5, 1, DocValueFormat.RAW, Map.of());

        StringTerms.Bucket appleBucket = new StringTerms.Bucket(
            new BytesRef("Apple"), 2,
            InternalAggregations.from(List.of(appleAvg)),
            false, 0, DocValueFormat.RAW);
        StringTerms.Bucket samsungBucket = new StringTerms.Bucket(
            new BytesRef("Samsung"), 1,
            InternalAggregations.from(List.of(samsungAvg)),
            false, 0, DocValueFormat.RAW);

        StringTerms termsAgg = new StringTerms(
            "by_brand",
            BucketOrder.count(false),
            BucketOrder.count(false),
            Map.of(),
            DocValueFormat.RAW,
            10,
            false,
            0,
            List.of(appleBucket, samsungBucket),
            0,
            new TermsAggregator.BucketCountThresholds(1, 0, 10, 10)
        );

        return InternalAggregations.from(List.of(termsAgg));
    }

    private static SearchHit createDummyHit(int docId, String brand, double price) {
        Map<String, Object> sourceMap = new LinkedHashMap<>();
        sourceMap.put("brand", brand);
        sourceMap.put("price", price);
        sourceMap.put("status", "active");

        Map<String, DocumentField> fields = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : sourceMap.entrySet()) {
            fields.put(e.getKey(), new DocumentField(e.getKey(), List.of(e.getValue())));
        }
        return createHit(docId, fields, sourceMap);
    }

    private static SearchHit createHit(int docId, Map<String, DocumentField> fields, Map<String, Object> sourceMap) {
        SearchHit hit = new SearchHit(docId, String.valueOf(docId), fields, Map.of());
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().map(sourceMap);
            hit.sourceRef(BytesReference.bytes(builder));
        } catch (IOException e) {
            // fallback — no source
        }
        return hit;
    }

    private static SearchResponse wrapInResponse(SearchHit[] hits, InternalAggregations aggs, long tookInMillis) {
        SearchHits searchHits = new SearchHits(
            hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), Float.NaN);
        InternalSearchResponse internal = new InternalSearchResponse(
            searchHits, aggs, null, null, false, null, 1);
        return new SearchResponse(
            internal, null, 1, 1, 0, tookInMillis,
            ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }
}
