/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * REST integration tests for DSL queries executed through the DataFusion backend.
 * <p>
 * Exercises: DSL _search interception → SearchSourceConverter → Calcite planning →
 * backend marking → DAG construction → Substrait fragment conversion → shard dispatch →
 * DataFusion native execution → Arrow result collection → SearchResponse.
 * <p>
 * Uses the same schema (id, name, age, score, city) and mock parquet data as
 * {@link DataFusionScanRestIT}.
 */
public class DslDataFusionRestIT extends OpenSearchRestTestCase {

    private static final Logger logger = LogManager.getLogger(DslDataFusionRestIT.class);
    private static final String TEST_INDEX = "parquet_simple";

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    private void createTestIndex() throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + TEST_INDEX));
        } catch (Exception e) {
            // index doesn't exist, ignore
        }
        Request createIndex = new Request("PUT", "/" + TEST_INDEX);
        createIndex.setJsonEntity(INDEX_MAPPING);
        client().performRequest(createIndex);
    }

    private void waitForClusterHealth() throws IOException {
        Request healthRequest = new Request("GET", "/_cluster/health/" + TEST_INDEX);
        healthRequest.addParameter("wait_for_status", "yellow");
        healthRequest.addParameter("timeout", "60s");
        client().performRequest(healthRequest);
    }

    public void testDslDataFusionQueries() throws Exception {
        createTestIndex();
        waitForClusterHealth();

        List<String> failures = new ArrayList<>();

        // q1: match_all — basic scan through the Calcite pipeline
        try {
            runQuery("q1", "{\"query\": {\"match_all\": {}}}", result -> {
                assertNotNull("q1: response should contain 'hits'", result.get("hits"));
                assertFalse("q1: should not time out", (Boolean) result.get("timed_out"));
                logger.info("q1: hits={}", result.get("hits"));
            });
        } catch (Exception e) {
            failures.add("q1: " + e.getMessage());
            logger.error("FAILED q1", e);
        }

        // q2: match_all with size
        try {
            runQuery("q2", "{\"size\": 5, \"query\": {\"match_all\": {}}}", result -> {
                assertNotNull("q2: response should contain 'hits'", result.get("hits"));
                assertFalse("q2: should not time out", (Boolean) result.get("timed_out"));
                logger.info("q2: hits={}", result.get("hits"));
            });
        } catch (Exception e) {
            failures.add("q2: " + e.getMessage());
            logger.error("FAILED q2", e);
        }

        // q3: term filter
        try {
            runQuery("q3", "{\"query\": {\"term\": {\"city\": \"paris\"}}}", result -> {
                assertNotNull("q3: response should contain 'hits'", result.get("hits"));
                assertFalse("q3: should not time out", (Boolean) result.get("timed_out"));
                logger.info("q3: hits={}", result.get("hits"));
            });
        } catch (Exception e) {
            failures.add("q3: " + e.getMessage());
            logger.error("FAILED q3", e);
        }

        // TODO: q4 disabled — OpenSearchAggregateSplitRule type mismatch bug.
        // q4: aggregation (count by city)
        // try {
        //     runQuery("q4", "{\"size\": 0, \"aggs\": {\"cities\": {\"terms\": {\"field\": \"city\"}}}}", result -> {
        //         assertNotNull("q4: response should contain 'hits'", result.get("hits"));
        //         assertFalse("q4: should not time out", (Boolean) result.get("timed_out"));
        //         logger.info("q4: hits={}", result.get("hits"));
        //     });
        // } catch (Exception e) {
        //     failures.add("q4: " + e.getMessage());
        //     logger.error("FAILED q4", e);
        // }

        // q5: projection (_source filtering)
        try {
            runQuery("q5", "{\"_source\": [\"name\", \"city\"], \"query\": {\"match_all\": {}}}", result -> {
                assertNotNull("q5: response should contain 'hits'", result.get("hits"));
                assertFalse("q5: should not time out", (Boolean) result.get("timed_out"));
                logger.info("q5: hits={}", result.get("hits"));
            });
        } catch (Exception e) {
            failures.add("q5: " + e.getMessage());
            logger.error("FAILED q5", e);
        }

        // q6: sort by age descending
        try {
            runQuery("q6", "{\"sort\": [{\"age\": \"desc\"}], \"query\": {\"match_all\": {}}}", result -> {
                assertNotNull("q6: response should contain 'hits'", result.get("hits"));
                assertFalse("q6: should not time out", (Boolean) result.get("timed_out"));
                logger.info("q6: hits={}", result.get("hits"));
            });
        } catch (Exception e) {
            failures.add("q6: " + e.getMessage());
            logger.error("FAILED q6", e);
        }

        if (failures.isEmpty() == false) {
            fail(failures.size() + " DSL-DataFusion queries failed:\n  " + String.join("\n  ", failures));
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void runQuery(String queryId, String dslBody, QueryAssertion assertion) throws Exception {
        logger.info("=== DSL-DataFusion {} ===\nBody: {}", queryId, dslBody);

        Request request = new Request("GET", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(dslBody);
        Response response = client().performRequest(request);

        assertEquals("HTTP status for " + queryId, 200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        logger.info("RESULT {}: {}", queryId, responseMap);

        assertion.verify(responseMap);
        logger.info("SUCCESS {}", queryId);
    }

    @FunctionalInterface
    private interface QueryAssertion {
        void verify(Map<String, Object> result) throws Exception;
    }

    // ── Index mapping ────────────────────────────────────────────────────────

    private static final String INDEX_MAPPING = "{\n"
        + "  \"settings\": {\n"
        + "    \"index.number_of_shards\": 1,\n"
        + "    \"index.number_of_replicas\": 0\n"
        + "  },\n"
        + "  \"mappings\": {\n"
        + "    \"properties\": {\n"
        + "      \"id\": {\"type\": \"long\"},\n"
        + "      \"name\": {\"type\": \"keyword\"},\n"
        + "      \"age\": {\"type\": \"long\"},\n"
        + "      \"score\": {\"type\": \"long\"},\n"
        + "      \"city\": {\"type\": \"keyword\"}\n"
        + "    }\n"
        + "  }\n"
        + "}";
}
