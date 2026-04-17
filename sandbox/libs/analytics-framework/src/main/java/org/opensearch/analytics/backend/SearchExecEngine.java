/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.io.IOException;

/**
 * Shard-level search execution engine interface.
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SearchExecEngine extends Closeable {

    /**
     * Executes the query and returns a result stream.
     */
    EngineResultStream execute() throws IOException;

    @Override
    default void close() throws IOException {}
}
