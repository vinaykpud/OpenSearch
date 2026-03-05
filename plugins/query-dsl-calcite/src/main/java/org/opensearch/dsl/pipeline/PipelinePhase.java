/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.pipeline;

/**
 * Defines the execution phases of the DSL-to-Calcite conversion pipeline.
 *
 * Each {@link ClauseConverter} declares its phase, and the
 * {@link ConversionPipeline} auto-sorts converters by {@link #getOrder()} at build time.
 *
 * Order values use gaps of 100 so new phases can be inserted between
 * existing ones without renumbering (e.g., HAVING at 450 between AGGREGATE and POST_AGGREGATE).
 */
public enum PipelinePhase {
    SCHEMA(100),
    FILTER(200),
    SORT(300),
    AGGREGATE(400),
    POST_AGGREGATE(500),
    PROJECT(600),
    LIMIT(700);

    private final int order;

    PipelinePhase(int order) {
        this.order = order;
    }

    public int getOrder() {
        return order;
    }
}
