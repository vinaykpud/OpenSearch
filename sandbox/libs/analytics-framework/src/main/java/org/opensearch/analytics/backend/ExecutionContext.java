/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.tasks.CancellableTask;

/**
 * Execution context carrying reader and plan state through
 * the query execution lifecycle.
 *
 * @opensearch.internal
 */
public class ExecutionContext {

    private final String tableName;
    private final Reader reader;
    private final CancellableTask task;
    private byte[] fragmentBytes;

    /**
     * Constructs an execution context.
     * @param tableName the target table name
     * @param task the cancellable task (SearchShardTask or AnalyticsShardTask)
     * @param reader the data-format aware reader
     */
    public ExecutionContext(String tableName, CancellableTask task, Reader reader) {
        this.tableName = tableName;
        this.task = task;
        this.reader = reader;
    }

    /** Returns the cancellable task. */
    public CancellableTask getTask() {
        return task;
    }

    /** Returns the target table name. */
    public String getTableName() {
        return tableName;
    }

    /** Returns the data-format aware reader. */
    public Reader getReader() {
        return reader;
    }

    /** Returns the backend-specific serialized plan fragment bytes, or null if not set. */
    public byte[] getFragmentBytes() {
        return fragmentBytes;
    }

    /** Sets the backend-specific serialized plan fragment bytes. */
    public void setFragmentBytes(byte[] fragmentBytes) {
        this.fragmentBytes = fragmentBytes;
    }
}
