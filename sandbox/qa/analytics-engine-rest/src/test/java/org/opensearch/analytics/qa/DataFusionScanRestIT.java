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
 * REST integration tests for the DataFusion backend through the full unified pipeline.
 * <p>
 * Uses a simple schema (id, name, age, score, city) matching the mock parquet data
 * and exercises: PPL parsing → Calcite planning → backend marking → DAG construction →
 * Substrait fragment conversion → shard dispatch → DataFusion native execution →
 * Arrow result collection → REST response.
 */
public class DataFusionScanRestIT extends OpenSearchRestTestCase {

    private static final Logger logger = LogManager.getLogger(DataFusionScanRestIT.class);
    private static final String TEST_INDEX = "parquet_simple";

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    /**
     * Creates the index with the expected schema. No data ingestion is needed —
     * the DataFusion backend uses a mock parquet reader that serves 100 rows
     * of pre-generated data (id, name, age, score, city) regardless of index contents.
     */
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

    public void testDataFusionQueries() throws Exception {
        createTestIndex();
        waitForClusterHealth();

        List<String> failures = new ArrayList<>();

        // q1: COUNT(*) → 100 (mock parquet has 100 rows)
        try {
            runQuery("q1", "source=" + TEST_INDEX + " | stats count() as cnt", result -> {
                assertColumns(result, "q1", "cnt");
                assertSingleRow(result, "q1", "cnt", 100);
            });
        } catch (Exception e) {
            failures.add("q1: " + e.getMessage());
            logger.error("FAILED q1", e);
        }

        // q2: SUM(age) → 4228
        try {
            runQuery("q2", "source=" + TEST_INDEX + " | stats sum(age) as total_age", result -> {
                assertColumns(result, "q2", "total_age");
                assertSingleRow(result, "q2", "total_age", 4228);
            });
        } catch (Exception e) {
            failures.add("q2: " + e.getMessage());
            logger.error("FAILED q2", e);
        }

        // q3: COUNT(*) GROUP BY city → 5 cities
        try {
            runQuery("q3", "source=" + TEST_INDEX + " | stats count() as cnt by city", result -> {
                List<String> columns = getColumnNames(result);
                @SuppressWarnings("unchecked")
                List<List<Object>> rows = (List<List<Object>>) result.get("datarows");

                int cntIdx = columns.indexOf("cnt");
                int cityIdx = columns.indexOf("city");
                assertTrue("q3: columns must contain 'cnt'", cntIdx >= 0);
                assertTrue("q3: columns must contain 'city'", cityIdx >= 0);
                assertEquals("q3: expected 5 group rows", 5, rows.size());

                boolean foundParis = false, foundTokyo = false, foundBerlin = false;
                boolean foundNewYork = false, foundLondon = false;
                for (List<Object> row : rows) {
                    String city = String.valueOf(row.get(cityIdx));
                    long cnt = ((Number) row.get(cntIdx)).longValue();
                    switch (city) {
                        case "paris": assertEquals("q3: paris count", 12, cnt); foundParis = true; break;
                        case "tokyo": assertEquals("q3: tokyo count", 22, cnt); foundTokyo = true; break;
                        case "berlin": assertEquals("q3: berlin count", 26, cnt); foundBerlin = true; break;
                        case "new york": assertEquals("q3: new york count", 22, cnt); foundNewYork = true; break;
                        case "london": assertEquals("q3: london count", 18, cnt); foundLondon = true; break;
                        default: fail("q3: unexpected city: " + city);
                    }
                }
                assertTrue("q3: missing paris row", foundParis);
                assertTrue("q3: missing tokyo row", foundTokyo);
                assertTrue("q3: missing berlin row", foundBerlin);
                assertTrue("q3: missing new york row", foundNewYork);
                assertTrue("q3: missing london row", foundLondon);
            });
        } catch (Exception e) {
            failures.add("q3: " + e.getMessage());
            logger.error("FAILED q3", e);
        }

        // q4: MIN/MAX age → min=18, max=65
        try {
            runQuery("q4", "source=" + TEST_INDEX + " | stats min(age) as min_age, max(age) as max_age", result -> {
                assertColumns(result, "q4", "min_age", "max_age");

                List<String> columns = getColumnNames(result);
                @SuppressWarnings("unchecked")
                List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
                assertEquals("q4: expected 1 row", 1, rows.size());

                int minIdx = columns.indexOf("min_age");
                int maxIdx = columns.indexOf("max_age");
                assertEquals("q4: min_age", 18, ((Number) rows.get(0).get(minIdx)).longValue());
                assertEquals("q4: max_age", 65, ((Number) rows.get(0).get(maxIdx)).longValue());
            });
        } catch (Exception e) {
            failures.add("q4: " + e.getMessage());
            logger.error("FAILED q4", e);
        }

        // q5: projection → 100 rows, 2 columns (name, city)
        try {
            runQuery("q5", "source=" + TEST_INDEX + " | fields name, city", result -> {
                List<String> columns = getColumnNames(result);
                @SuppressWarnings("unchecked")
                List<List<Object>> rows = (List<List<Object>>) result.get("datarows");

                assertEquals("q5: expected 2 columns", 2, columns.size());
                assertTrue("q5: columns must contain 'name'", columns.contains("name"));
                assertTrue("q5: columns must contain 'city'", columns.contains("city"));
                assertEquals("q5: expected 100 rows", 100, rows.size());
            });
        } catch (Exception e) {
            failures.add("q5: " + e.getMessage());
            logger.error("FAILED q5", e);
        }

        if (failures.isEmpty() == false) {
            fail(failures.size() + " DataFusion queries failed:\n  " + String.join("\n  ", failures));
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void runQuery(String queryId, String ppl, QueryAssertion assertion) throws Exception {
        logger.info("=== DataFusion {} ===\nPPL: {}", queryId, ppl);

        Request request = new Request("POST", "/_plugins/_ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
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

    /** Extracts column names from the schema list of {"name":..., "type":...} maps. */
    @SuppressWarnings("unchecked")
    private List<String> getColumnNames(Map<String, Object> result) {
        List<Map<String, Object>> schema = (List<Map<String, Object>>) result.get("schema");
        List<String> names = new ArrayList<>();
        for (Map<String, Object> field : schema) {
            names.add((String) field.get("name"));
        }
        return names;
    }

    private void assertColumns(Map<String, Object> result, String queryId, String... expectedColumns) {
        assertNotNull(queryId + ": response should contain 'schema'", result.get("schema"));
        List<String> columns = getColumnNames(result);
        for (String col : expectedColumns) {
            assertTrue(queryId + ": columns must contain '" + col + "'", columns.contains(col));
        }
    }

    private void assertSingleRow(Map<String, Object> result, String queryId, String column, long expected) {
        assertNotNull(queryId + ": response should contain 'datarows'", result.get("datarows"));
        List<String> columns = getColumnNames(result);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertEquals(queryId + ": expected 1 row", 1, rows.size());
        int idx = columns.indexOf(column);
        assertTrue(queryId + ": column '" + column + "' not found", idx >= 0);
        long actual = ((Number) rows.get(0).get(idx)).longValue();
        assertEquals(queryId + ": " + column, expected, actual);
    }

    private static String escapeJson(String text) {
        return text.replace("\\", "\\\\").replace("\"", "\\\"");
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
