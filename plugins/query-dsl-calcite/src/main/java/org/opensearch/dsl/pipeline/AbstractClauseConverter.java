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
 * Template Method base class for DSL converters.
 *
 * Enforces a 3-phase lifecycle for every converter:
 * <ol>
 *   <li><b>Applicability</b> — {@link #isApplicable(ConversionContext)}: skip if not needed</li>
 *   <li><b>Validation</b> — {@link #validate(ConversionContext)}: check downstream capabilities</li>
 *   <li><b>Conversion</b> — {@link #doConvert(RelNode, ConversionContext)}: perform the conversion</li>
 * </ol>
 */
public abstract class AbstractClauseConverter implements ClauseConverter {

    private final PipelinePhase phase;

    protected AbstractClauseConverter(PipelinePhase phase) {
        this.phase = phase;
    }

    @Override
    public final PipelinePhase getPhase() {
        return phase;
    }

    @Override
    public final RelNode convert(RelNode input, ConversionContext ctx) throws ConversionException {
        if (!isApplicable(ctx)) {
            return input;
        }
        validate(ctx);
        return doConvert(input, ctx);
    }

    /**
     * Returns true if this converter should run for the given context.
     *
     * @param ctx The conversion context
     * @return true if the converter is applicable
     */
    protected abstract boolean isApplicable(ConversionContext ctx);

    /**
     * Validates that the downstream system can handle the output of this converter.
     * Default implementation is a no-op.
     *
     * @param ctx The conversion context
     * @throws ConversionException if downstream capabilities are insufficient
     */
    protected void validate(ConversionContext ctx) throws ConversionException {}

    /**
     * Performs the actual conversion.
     *
     * @param input The current RelNode
     * @param ctx The conversion context
     * @return The transformed RelNode
     * @throws ConversionException if the conversion fails
     */
    protected abstract RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException;
}
