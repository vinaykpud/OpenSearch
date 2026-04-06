/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.rel.RelNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.mock;

/**
 * Tests for {@link FragmentExecutionRequest} serialization and getters.
 */
public class FragmentExecutionRequestTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws IOException {
        String queryId = "query-123";
        int stageId = 7;
        String taskId = "task-abc";
        ShardId shardId = new ShardId(new Index("test-index", "uuid"), 0);
        String backendId = "lucene";
        byte[] fragmentBytes = "plan-bytes".getBytes(StandardCharsets.UTF_8);
        RelNode fragment = mock(RelNode.class);

        FragmentExecutionRequest original = new FragmentExecutionRequest(
            queryId,
            stageId,
            taskId,
            shardId,
            backendId,
            fragmentBytes,
            fragment
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        FragmentExecutionRequest deserialized = new FragmentExecutionRequest(in);

        assertEquals(queryId, deserialized.getQueryId());
        assertEquals(stageId, deserialized.getStageId());
        assertEquals(taskId, deserialized.getTaskId());
        assertEquals(shardId, deserialized.getShardId());
        assertEquals(backendId, deserialized.getBackendId());
        assertArrayEquals(fragmentBytes, deserialized.getFragmentBytes());
        assertNull("Transient fragment should be null after deserialization", deserialized.getFragment());
    }

    public void testNullFragmentBytesRoundTrip() throws IOException {
        ShardId shardId = new ShardId(new Index("test-index", "uuid"), 0);

        FragmentExecutionRequest original = new FragmentExecutionRequest("query-456", 2, "task-def", shardId, "datafusion", null, null);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        FragmentExecutionRequest deserialized = new FragmentExecutionRequest(in);

        assertNull("fragmentBytes should be null after round-trip of null value", deserialized.getFragmentBytes());
        assertEquals("query-456", deserialized.getQueryId());
        assertEquals(2, deserialized.getStageId());
        assertEquals("task-def", deserialized.getTaskId());
        assertEquals(shardId, deserialized.getShardId());
        assertEquals("datafusion", deserialized.getBackendId());
    }

    public void testGetters() {
        String queryId = "query-789";
        int stageId = 3;
        String taskId = "task-ghi";
        ShardId shardId = new ShardId(new Index("my-index", "my-uuid"), 5);
        String backendId = "lucene";
        byte[] fragmentBytes = new byte[] { 1, 2, 3 };
        RelNode fragment = mock(RelNode.class);

        FragmentExecutionRequest request = new FragmentExecutionRequest(
            queryId,
            stageId,
            taskId,
            shardId,
            backendId,
            fragmentBytes,
            fragment
        );

        assertEquals(queryId, request.getQueryId());
        assertEquals(stageId, request.getStageId());
        assertEquals(taskId, request.getTaskId());
        assertEquals(shardId, request.getShardId());
        assertEquals(backendId, request.getBackendId());
        assertArrayEquals(fragmentBytes, request.getFragmentBytes());
        assertSame(fragment, request.getFragment());
    }
}
