/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link FragmentExecutionResponse} serialization and getters.
 */
public class FragmentExecutionResponseTests extends OpenSearchTestCase {

    public void testSerializationRoundTripMixedTypes() throws IOException {
        List<String> fieldNames = List.of("name", "count", "score", "rank", "nullable");
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { "alice", 42L, 3.14, 1, null });
        rows.add(new Object[] { "bob", 100L, 2.71, 2, null });

        FragmentExecutionResponse original = new FragmentExecutionResponse(fieldNames, rows);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        FragmentExecutionResponse deserialized = new FragmentExecutionResponse(in);

        assertEquals(fieldNames, deserialized.getFieldNames());
        assertEquals(rows.size(), deserialized.getRows().size());
        for (int r = 0; r < rows.size(); r++) {
            Object[] expectedRow = rows.get(r);
            Object[] actualRow = deserialized.getRows().get(r);
            assertEquals(expectedRow.length, actualRow.length);
            for (int c = 0; c < expectedRow.length; c++) {
                assertEquals(expectedRow[c], actualRow[c]);
            }
        }
    }

    public void testSerializationRoundTripEmptyRows() throws IOException {
        List<String> fieldNames = List.of("col_a", "col_b");
        List<Object[]> rows = new ArrayList<>();

        FragmentExecutionResponse original = new FragmentExecutionResponse(fieldNames, rows);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        FragmentExecutionResponse deserialized = new FragmentExecutionResponse(in);

        assertEquals(fieldNames, deserialized.getFieldNames());
        assertTrue(deserialized.getRows().isEmpty());
    }

    public void testGetters() {
        List<String> fieldNames = List.of("x", "y");
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { "hello", 99L });

        FragmentExecutionResponse response = new FragmentExecutionResponse(fieldNames, rows);

        assertEquals(fieldNames, response.getFieldNames());
        assertEquals(1, response.getRows().size());
        assertArrayEquals(new Object[] { "hello", 99L }, response.getRows().get(0));
    }
}
