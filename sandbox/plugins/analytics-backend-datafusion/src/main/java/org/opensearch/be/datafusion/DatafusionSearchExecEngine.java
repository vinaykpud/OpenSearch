/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * DataFusion-backed search execution engine.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionSearchExecEngine implements SearchExecEngine {

    private final long readerPtr;
    private final String tableName;
    private final byte[] substraitBytes;
    private final NativeRuntimeHandle nativeRuntime;
    private final Supplier<BufferAllocator> allocatorFactory;

    /**
     * Creates a fully configured DataFusion execution engine.
     *
     * @param readerPtr        native reader pointer (NOT owned — lifecycle managed by DatafusionReaderManager)
     * @param tableName        target table/index name
     * @param substraitBytes   serialized Substrait plan bytes
     * @param nativeRuntime    handle to the native DataFusion runtime
     * @param allocatorFactory factory for creating Arrow buffer allocators for result streams
     */
    public DatafusionSearchExecEngine(
        long readerPtr,
        String tableName,
        byte[] substraitBytes,
        NativeRuntimeHandle nativeRuntime,
        Supplier<BufferAllocator> allocatorFactory
    ) {
        this.readerPtr = readerPtr;
        this.tableName = tableName;
        this.substraitBytes = substraitBytes;
        this.nativeRuntime = nativeRuntime;
        this.allocatorFactory = allocatorFactory;
    }

    @Override
    public EngineResultStream execute() throws IOException {
        CompletableFuture<Long> future = new CompletableFuture<>();
        NativeBridge.executeQueryAsync(readerPtr, tableName, substraitBytes, nativeRuntime.get(), new ActionListener<>() {
            @Override
            public void onResponse(Long streamPtr) {
                future.complete(streamPtr);
            }

            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(e);
            }
        });

        long streamPtr;
        try {
            streamPtr = future.join();
        } catch (Exception e) {
            throw new IOException("Query execution failed", e);
        }

        StreamHandle handle = new StreamHandle(streamPtr, nativeRuntime);
        try {
            return new DatafusionResultStream(handle, allocatorFactory.get());
        } catch (Exception e) {
            handle.close();
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        // StreamHandle is owned by DatafusionResultStream — nothing to clean up here.
        // readerPtr is NOT owned by this engine (managed by DatafusionReaderManager).
    }
}
