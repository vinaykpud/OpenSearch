/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Context information for physical planning.
 *
 * <p>Contains metadata needed during physical plan generation:
 * <ul>
 *   <li>Schema information (table definitions, field types)</li>
 *   <li>Statistics (row counts, cardinalities)</li>
 *   <li>Index information (available indexes, doc values)</li>
 *   <li>Configuration options</li>
 * </ul>
 */
public class PlanningContext {

    private final Map<String, Object> metadata;

    /**
     * Creates a new planning context with the given metadata.
     *
     * @param metadata the metadata map
     */
    public PlanningContext(Map<String, Object> metadata) {
        this.metadata = new HashMap<>(metadata);
    }

    /**
     * Gets a metadata value by key.
     *
     * @param key the metadata key
     * @return the metadata value, or null if not found
     */
    public Object get(String key) {
        return metadata.get(key);
    }

    /**
     * Gets a metadata value by key with a default value.
     *
     * @param key the metadata key
     * @param defaultValue the default value if key not found
     * @param <T> the value type
     * @return the metadata value, or defaultValue if not found
     */
    @SuppressWarnings("unchecked")
    public <T> T get(String key, T defaultValue) {
        Object value = metadata.get(key);
        return value != null ? (T) value : defaultValue;
    }

    /**
     * Gets all metadata as an unmodifiable map.
     *
     * @return the metadata map
     */
    public Map<String, Object> getMetadata() {
        return Collections.unmodifiableMap(metadata);
    }

    /**
     * Creates a default planning context with no metadata.
     *
     * @return a new default planning context
     */
    public static PlanningContext createDefault() {
        return new PlanningContext(Collections.emptyMap());
    }

    /**
     * Creates a builder for constructing planning contexts.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for creating planning contexts.
     */
    public static class Builder {
        private final Map<String, Object> metadata = new HashMap<>();

        /**
         * Constructs a new Builder.
         */
        public Builder() {
            // Default constructor
        }

        /**
         * Adds a metadata entry.
         *
         * @param key the metadata key
         * @param value the metadata value
         * @return this builder
         */
        public Builder withMetadata(String key, Object value) {
            metadata.put(key, value);
            return this;
        }

        /**
         * Adds all metadata from a map.
         *
         * @param metadata the metadata map
         * @return this builder
         */
        public Builder withMetadata(Map<String, Object> metadata) {
            this.metadata.putAll(metadata);
            return this;
        }

        /**
         * Builds the planning context.
         *
         * @return a new planning context
         */
        public PlanningContext build() {
            return new PlanningContext(metadata);
        }
    }
}
