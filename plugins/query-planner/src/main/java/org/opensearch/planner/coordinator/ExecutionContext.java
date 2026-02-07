/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.coordinator;

import org.opensearch.index.shard.IndexShard;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Execution context providing access to OpenSearch infrastructure.
 *
 * <p>The execution context contains:
 * <ul>
 *   <li>IndexShard for accessing Lucene indexes</li>
 *   <li>Metadata for query execution</li>
 *   <li>Configuration options</li>
 * </ul>
 *
 * <p>This class provides a bridge between the query planner and
 * OpenSearch's existing search infrastructure.
 *
 * <p><b>Example:</b>
 * <pre>
 * ExecutionContext context = ExecutionContext.builder()
 *     .withIndexShard(indexShard)
 *     .withMetadata("indexName", "products")
 *     .build();
 * </pre>
 */
public class ExecutionContext {

    private final IndexShard indexShard;
    private final Map<String, Object> metadata;

    /**
     * Constructs a new ExecutionContext.
     *
     * @param indexShard the index shard for Lucene access
     * @param metadata additional metadata
     */
    private ExecutionContext(IndexShard indexShard, Map<String, Object> metadata) {
        this.indexShard = indexShard;
        this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
    }

    /**
     * Gets the index shard.
     *
     * @return the index shard, or null if not available
     */
    public IndexShard getIndexShard() {
        return indexShard;
    }

    /**
     * Gets metadata value by key.
     *
     * @param key the metadata key
     * @return the metadata value, or null if not found
     */
    public Object getMetadata(String key) {
        return metadata.get(key);
    }

    /**
     * Gets all metadata.
     *
     * @return unmodifiable map of metadata
     */
    public Map<String, Object> getAllMetadata() {
        return Map.copyOf(metadata);
    }

    /**
     * Creates a new builder for ExecutionContext.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for ExecutionContext.
     */
    public static class Builder {
        
        /**
         * Constructs a new Builder.
         */
        public Builder() {
            // Default constructor
        }
        private IndexShard indexShard;
        private Map<String, Object> metadata = new HashMap<>();

        /**
         * Sets the index shard.
         *
         * @param indexShard the index shard
         * @return this builder
         */
        public Builder withIndexShard(IndexShard indexShard) {
            this.indexShard = indexShard;
            return this;
        }

        /**
         * Adds metadata.
         *
         * @param key the metadata key
         * @param value the metadata value
         * @return this builder
         */
        public Builder withMetadata(String key, Object value) {
            this.metadata.put(key, value);
            return this;
        }

        /**
         * Builds the ExecutionContext.
         *
         * @return the execution context
         */
        public ExecutionContext build() {
            return new ExecutionContext(indexShard, metadata);
        }
    }

    @Override
    public String toString() {
        return String.format(
            "ExecutionContext[hasIndexShard=%b, metadata=%s]",
            indexShard != null,
            metadata.keySet()
        );
    }
}
