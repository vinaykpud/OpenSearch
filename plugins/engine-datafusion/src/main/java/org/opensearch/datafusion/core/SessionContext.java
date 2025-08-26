/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.core;

import org.opensearch.datafusion.DataFusionJNI;

/**
 * Session context for datafusion
 */
public class SessionContext implements AutoCloseable {

    // ptr to context in df
    private final long ptr;

    /**
     * Default constructor for SessionContext.
     * Creates a context with a default parquet file path.
     */
    public SessionContext() {
        // Use a default parquet file path for now
        String defaultParquetPath = "/tmp/sample.parquet";
        this.ptr = DataFusionJNI.nativeCreateContext(defaultParquetPath);
    }

    /**
     * Constructor for SessionContext with custom parquet file.
     * @param parquetFilePath Path to the parquet file to register
     */
    public SessionContext(String parquetFilePath) {
        this.ptr = DataFusionJNI.nativeCreateContext(parquetFilePath);
    }

    /**
     * Get the native context pointer
     * @return the context pointer
     */
    public long getContextId() {
        return ptr;
    }

    @Override
    public void close() throws Exception {
        if (ptr != 0) {
            DataFusionJNI.nativeCloseContext(this.ptr);
        }
    }
}
