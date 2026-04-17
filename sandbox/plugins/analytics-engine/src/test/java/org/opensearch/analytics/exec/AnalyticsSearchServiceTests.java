/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;

import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AnalyticsSearchService}.
 */
public class AnalyticsSearchServiceTests extends OpenSearchTestCase {

    /**
     * Happy path: executeFragment selects the matching backend, acquires a reader,
     * executes the engine, drains the result stream, and returns fieldNames + rows.
     */
    public void testExecuteFragmentReturnsRows() throws IOException {
        // Setup mock backend
        StubBackendPlugin backend = new StubBackendPlugin();
        AnalyticsSearchService service = new AnalyticsSearchService(Map.of("test-backend", backend));

        // Build request
        ShardId shardId = new ShardId(new Index("test_index", "uuid"), 0);
        byte[] planBytes = new byte[] { 1, 2, 3 };
        FragmentExecutionRequest request = new FragmentExecutionRequest(
            "query-1",
            0,
            shardId,
            List.of(new FragmentExecutionRequest.PlanAlternative("test-backend", planBytes))
        );

        // Mock shard -> readerProvider -> reader
        IndexShard shard = mock(IndexShard.class);
        when(shard.shardId()).thenReturn(shardId);

        IndexReaderProvider readerProvider = mock(IndexReaderProvider.class);
        when(shard.getReaderProvider()).thenReturn(readerProvider);

        IndexReaderProvider.Reader reader = new StubReader();
        when(readerProvider.acquireReader()).thenReturn(new GatedCloseable<>(reader, () -> {}));

        // Execute
        FragmentExecutionResponse response = service.executeFragment(request, shard);

        // Verify
        assertEquals(List.of("col_a", "col_b"), response.getFieldNames());
        assertEquals(3, response.getRows().size());
        assertArrayEquals(new Object[] { "row_0_col_a", "row_0_col_b" }, response.getRows().get(0));
        assertArrayEquals(new Object[] { "row_1_col_a", "row_1_col_b" }, response.getRows().get(1));
        assertArrayEquals(new Object[] { "row_2_col_a", "row_2_col_b" }, response.getRows().get(2));
    }

    // -- Stubs --

    /** Minimal Reader stub — AnalyticsSearchService only passes it through to ExecutionContext. */
    static class StubReader implements IndexReaderProvider.Reader {
        @Override
        public Object reader(DataFormat format) {
            return null;
        }

        @Override
        public <R> R getReader(DataFormat format, Class<R> readerType) {
            return null;
        }

        @Override
        public CatalogSnapshot catalogSnapshot() {
            return null;
        }

        @Override
        public void close() throws IOException {}
    }

    /** Backend plugin that returns a StubSearchExecEngine. */
    static class StubBackendPlugin implements AnalyticsSearchBackendPlugin {
        @Override
        public String name() {
            return "test-backend";
        }

        @Override
        public org.opensearch.analytics.spi.SearchExecEngineProvider getSearchExecEngineProvider() {
            return ctx -> new StubSearchExecEngine();
        }
    }

    static class StubSearchExecEngine implements SearchExecEngine {
        @Override
        public EngineResultStream execute() {
            return new StubResultStream();
        }

        @Override
        public void close() {}
    }

    static class StubResultStream implements EngineResultStream {
        @Override
        public Iterator<EngineResultBatch> iterator() {
            return new Iterator<>() {
                private boolean consumed = false;

                @Override
                public boolean hasNext() {
                    return !consumed;
                }

                @Override
                public EngineResultBatch next() {
                    consumed = true;
                    return new StubResultBatch();
                }
            };
        }

        @Override
        public void close() {}
    }

    /** Returns a fixed batch of 3 rows with columns [col_a, col_b]. */
    static class StubResultBatch implements EngineResultBatch {
        private static final int ROW_COUNT = 3;

        @Override
        public List<String> getFieldNames() {
            return List.of("col_a", "col_b");
        }

        @Override
        public int getRowCount() {
            return ROW_COUNT;
        }

        @Override
        public Object getFieldValue(String fieldName, int rowIndex) {
            return "row_" + rowIndex + "_" + fieldName;
        }
    }
}
