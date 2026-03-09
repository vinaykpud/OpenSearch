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
 * Each {@link DslConverter} declares its phase, and the
 * {@link ConversionPipeline} auto-sorts converters by {@link #getOrder()} at build time.
 *
 * Order values use gaps of 100 so new phases can be inserted between
 * existing ones without renumbering (e.g., HAVING at 450 between AGGREGATE and POST_AGGREGATE).
 */
public enum PipelinePhase {
    /** Table scan phase (order 100). */
    SCAN(100),
    /** Filter/where-clause phase (order 200). */
    FILTER(200),
    /** Aggregation phase (order 400). */
    AGGREGATE(400),
    /** Post-aggregation phase (order 500). */
    POST_AGGREGATE(500),
    /** Projection/_source phase (order 600). */
    PROJECT(600),
    /** Sort and pagination phase (order 700). */
    SORT(700);

    private final int order;

    PipelinePhase(int order) {
        this.order = order;
    }

    /**
     * Returns the numeric order used to sort converters within the pipeline.
     *
     * @return the phase order value
     */
    public int getOrder() {
        return order;
    }
}
