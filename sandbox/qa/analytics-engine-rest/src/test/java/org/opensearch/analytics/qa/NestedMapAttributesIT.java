/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

import java.util.List;
import java.util.Map;

/**
 * End-to-end IT for OTel-style dynamic attributes stored as a parquet {@code MAP<Utf8,Utf8>}
 * (design/nested-map-attributes). A {@code nested} {@code events} field carries an {@code attributes}
 * sub-object mapped as {@code flat_object}; each event's open key space becomes one MAP column inside
 * the element struct.
 *
 * <p>Covers: the mapping stays static under divergent dynamic keys (no explosion); whole-{@code events}
 * projection returns the maps; group-by a map <em>value</em> works; and group-by the map itself / the
 * whole nested list is rejected at PLANNING time with a clear message (the arrow-row {@code Map}-in-key
 * limitation, guarded so it never reaches the engine as an opaque execution error).
 */
public class NestedMapAttributesIT extends AnalyticsRestTestCase {

    private static final String INDEX = "otel_map_it";

    private void createAndLoad() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignore) {
            // index may not exist
        }

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"lucene\""
            + "},"
            + "\"mappings\": { \"properties\": {"
            + "  \"traceId\": { \"type\": \"keyword\" },"
            + "  \"events\": { \"type\": \"nested\", \"properties\": {"
            + "    \"name\": { \"type\": \"keyword\", \"ignore_above\": 256 },"
            + "    \"attributes\": { \"type\": \"flat_object\" },"
            + "    \"droppedAttributesCount\": { \"type\": \"integer\" },"
            + "    \"time\": { \"type\": \"date_nanos\" }"
            + "  } }"
            + "} }"
            + "}";

        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(body);
        Map<String, Object> createResponse = assertOkAndParse(client().performRequest(create), "Create map index");
        assertEquals("Index creation should be acknowledged", true, createResponse.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);

        // Two traces with DIFFERENT dynamic attribute keys per event — proves the map keeps them all.
        String bulk = "{\"index\": {}}\n"
            + "{\"traceId\":\"a1b2c3d4e5f60718\",\"events\":["
            + "{\"name\":\"http\",\"droppedAttributesCount\":0,\"time\":\"2026-08-26T22:55:53.100000000Z\","
            + "\"attributes\":{\"http.method\":\"POST\",\"http.status_code\":\"500\"}},"
            + "{\"name\":\"db\",\"droppedAttributesCount\":1,\"time\":\"2026-08-26T22:55:53.400000000Z\","
            + "\"attributes\":{\"db.system\":\"postgresql\"}}]}\n"
            + "{\"index\": {}}\n"
            + "{\"traceId\":\"bb00cc11dd22ee33\",\"events\":["
            + "{\"name\":\"retry\",\"droppedAttributesCount\":0,\"time\":\"2026-08-26T22:55:54.000000000Z\","
            + "\"attributes\":{\"retry.reason\":\"deadline_exceeded\"}}]}\n";

        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk);
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setOptions(bulkRequest.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> bulkResponse = assertOkAndParse(client().performRequest(bulkRequest), "Bulk index");
        assertEquals("Bulk indexing should have no errors", false, bulkResponse.get("errors"));

        // Dynamic keys must NOT explode the mapping — attributes stays a single flat_object field.
        Map<String, Object> mapping = assertOkAndParse(
            client().performRequest(new Request("GET", "/" + INDEX + "/_mapping")),
            "Get mapping"
        );
        assertEquals("flat_object", attributesType(mapping));
    }

    /** Whole-events projection returns the per-event maps; group-by a map value works (element grain). */
    public void testProjectionAndGroupByMapValue() throws Exception {
        createAndLoad();

        Map<String, Object> proj = executePpl("source=" + INDEX + " | fields traceId, events");
        assertEquals("parent grain: one row per trace", 2, ((Number) proj.get("total")).intValue());
        assertEquals("events projects as an array", "array", columnType(proj, "events"));
        // The maps must be present in the response (rendered as key/value entries).
        String projText = proj.toString();
        assertTrue("http.method key present in projected map", projText.contains("http.method"));
        assertTrue("db.system key present in projected map", projText.contains("db.system"));
        assertTrue("retry.reason key present in projected map", projText.contains("retry.reason"));

        // Group-by a MAP VALUE (a scalar) is allowed and correct: one event has db.system=postgresql.
        Map<String, Object> grp = executePpl("source=" + INDEX + " | stats count() as cnt by events.attributes.db.system");
        assertTrue("group-by map value returns the value", grp.toString().contains("postgresql"));
    }

    /** Group-by the map itself, or the whole nested list containing it, is rejected at planning (HTTP 400). */
    public void testGroupByMapRejectedAtPlanning() throws Exception {
        createAndLoad();

        for (String query : List.of(
            "source=" + INDEX + " | stats count() by events.attributes",
            "source=" + INDEX + " | stats count() by events"
        )) {
            ResponseException e = expectThrows(ResponseException.class, () -> executePpl(query));
            assertEquals(
                "group-by-on-map must be a client error, not an execution 500",
                400,
                e.getResponse().getStatusLine().getStatusCode()
            );
            assertTrue(
                "guardrail message should explain the MAP limitation; got: " + e.getMessage(),
                e.getMessage().contains("grouping by a MAP field")
            );
        }
    }

    @SuppressWarnings("unchecked")
    private static String attributesType(Map<String, Object> mapping) {
        Map<String, Object> idx = (Map<String, Object>) mapping.get(INDEX);
        Map<String, Object> mappings = (Map<String, Object>) idx.get("mappings");
        Map<String, Object> props = (Map<String, Object>) mappings.get("properties");
        Map<String, Object> events = (Map<String, Object>) props.get("events");
        Map<String, Object> eventProps = (Map<String, Object>) events.get("properties");
        Map<String, Object> attributes = (Map<String, Object>) eventProps.get("attributes");
        return (String) attributes.get("type");
    }

    @SuppressWarnings("unchecked")
    private static String columnType(Map<String, Object> pplResponse, String column) {
        for (Object col : (List<Object>) pplResponse.get("schema")) {
            Map<String, Object> c = (Map<String, Object>) col;
            if (column.equals(c.get("name"))) {
                return (String) c.get("type");
            }
        }
        return null;
    }
}
