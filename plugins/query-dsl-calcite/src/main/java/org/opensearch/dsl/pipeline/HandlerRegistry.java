/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.pipeline;

import org.opensearch.dsl.exception.ConversionException;

import java.util.HashMap;
import java.util.Map;

/**
 * Generic base for handler registries that map a key class to a handler.
 *
 * Provides O(1) lookup by class with a descriptive error when no handler is found.
 *
 * @param <K> The key type (e.g., QueryBuilder, AggregationBuilder)
 * @param <H> The handler type
 */
public abstract class HandlerRegistry<K, H> {

    private final Map<Class<? extends K>, H> handlers = new HashMap<>();
    private final String handlerKind;

    /**
     * Creates a registry for the given handler kind.
     *
     * @param handlerKind descriptive name used in error messages (e.g., "query", "aggregation")
     */
    protected HandlerRegistry(String handlerKind) {
        this.handlerKind = handlerKind;
    }

    /**
     * Registers a handler for the given key type.
     *
     * @param keyType the class of the key to map
     * @param handler the handler to associate with the key type
     */
    protected void doRegister(Class<? extends K> keyType, H handler) {
        handlers.put(keyType, handler);
    }

    /**
     * Finds the handler registered for the runtime type of the given key.
     *
     * @param key the key whose handler is requested
     * @return the matching handler
     * @throws ConversionException if no handler is registered for the key type
     */
    protected H findHandler(K key) throws ConversionException {
        @SuppressWarnings("unchecked")
        Class<? extends K> keyClass = (Class<? extends K>) key.getClass();
        H handler = handlers.get(keyClass);
        if (handler != null) {
            return handler;
        }
        throw new ConversionException(handlerKind,
            "Unsupported " + handlerKind + " type: " + key.getClass().getSimpleName());
    }
}
