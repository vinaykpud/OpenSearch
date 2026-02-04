/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.scheduler;

import org.apache.arrow.memory.BufferAllocator;

import java.time.Duration;

/**
 * Context for query execution scheduling.
 *
 * <p>Encapsulates runtime parameters needed by the scheduler to execute a query plan.
 */
public class SchedulerContext {

    private static final int DEFAULT_BATCH_SIZE = 1024;
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private final Duration timeout;
    private final int batchSize;
    private final BufferAllocator allocator;

    /**
     * Create a scheduler context with default batch size and timeout.
     */
    public SchedulerContext(BufferAllocator allocator) {
        this(DEFAULT_TIMEOUT, DEFAULT_BATCH_SIZE, allocator);
    }

    /**
     * Create a scheduler context with specified timeout.
     */
    public SchedulerContext(Duration timeout, BufferAllocator allocator) {
        this(timeout, DEFAULT_BATCH_SIZE, allocator);
    }

    /**
     * Create a scheduler context with all parameters.
     */
    public SchedulerContext(Duration timeout, int batchSize, BufferAllocator allocator) {
        this.timeout = timeout != null ? timeout : DEFAULT_TIMEOUT;
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
        this.allocator = allocator;
    }

    /**
     * Get the execution timeout.
     */
    public Duration getTimeout() {
        return timeout;
    }

    /**
     * Get the batch size for operators.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Get the Arrow memory allocator.
     */
    public BufferAllocator getAllocator() {
        return allocator;
    }
}
