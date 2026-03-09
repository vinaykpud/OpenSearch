/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.pipeline;

import org.apache.calcite.rel.RelNode;
import org.opensearch.dsl.exception.ConversionException;

/**
 * A single converter in the DSL-to-Calcite conversion pipeline.
 *
 * Each converter transforms a RelNode (or null for the initial converter) into a new RelNode,
 * using shared state from the {@link ConversionContext}.
 */
public interface DslConverter {

    /**
     * Returns the pipeline phase this converter belongs to.
     * The pipeline auto-sorts converters by phase at build time.
     */
    PipelinePhase getPhase();

    /**
     * Converts this DSL section into a Calcite RelNode.
     *
     * @param input The current RelNode (null for the first converter)
     * @param context The shared conversion context
     * @return The transformed RelNode
     * @throws ConversionException if the conversion fails
     */
    RelNode convert(RelNode input, ConversionContext context) throws ConversionException;
}
