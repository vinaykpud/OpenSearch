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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Executes a sequence of {@link DslConverter}s to build a Calcite RelNode tree.
 *
 * Converters are executed in order. Each converter receives the output of the previous converter
 * and the shared {@link ConversionContext}.
 */
public class ConversionPipeline {

    private final List<DslConverter> converters;

    private ConversionPipeline(List<DslConverter> converters) {
        this.converters = converters;
    }

    /**
     * Executes all converters in sequence.
     *
     * @param context The shared conversion context
     * @return The final RelNode
     * @throws ConversionException if any converter fails
     */
    public RelNode execute(ConversionContext context) throws ConversionException {
        return execute(context, null);
    }

    /**
     * Executes all converters in sequence, starting from an initial RelNode.
     * Used by suffix pipelines that continue from a shared prefix output.
     *
     * @param context The shared conversion context
     * @param initialInput The RelNode to start from (output of a shared prefix pipeline)
     * @return The final RelNode
     * @throws ConversionException if any converter fails
     */
    public RelNode execute(ConversionContext context, RelNode initialInput) throws ConversionException {
        RelNode relNode = initialInput;
        for (DslConverter converter : converters) {
            relNode = converter.convert(relNode, context);
        }
        return relNode;
    }

    /**
     * Builder for constructing a ConversionPipeline.
     */
    public static class Builder {
        /** Creates a new empty pipeline builder. */
        public Builder() {}

        private final List<DslConverter> converters = new ArrayList<>();

        /**
         * Adds a converter to the pipeline.
         *
         * @param converter the DSL converter to add
         * @return this builder for chaining
         */
        public Builder addConverter(DslConverter converter) {
            converters.add(converter);
            return this;
        }

        /**
         * Builds the pipeline, sorting converters by their phase order.
         *
         * @return the constructed {@link ConversionPipeline}
         */
        public ConversionPipeline build() {
            List<DslConverter> sorted = new ArrayList<>(converters);
            sorted.sort(Comparator.comparingInt(c -> c.getPhase().getOrder()));
            return new ConversionPipeline(List.copyOf(sorted));
        }
    }
}
