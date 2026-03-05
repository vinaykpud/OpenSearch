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
 * Executes a sequence of {@link ClauseConverter}s to build a Calcite RelNode tree.
 *
 * Converters are executed in order. Each converter receives the output of the previous converter
 * and the shared {@link ConversionContext}.
 */
public class ConversionPipeline {

    private final List<ClauseConverter> converters;

    private ConversionPipeline(List<ClauseConverter> converters) {
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
        RelNode relNode = null;
        for (ClauseConverter converter : converters) {
            relNode = converter.convert(relNode, context);
        }
        return relNode;
    }

    /**
     * Builder for constructing a ConversionPipeline.
     */
    public static class Builder {
        private final List<ClauseConverter> converters = new ArrayList<>();

        public Builder addConverter(ClauseConverter converter) {
            converters.add(converter);
            return this;
        }

        public ConversionPipeline build() {
            List<ClauseConverter> sorted = new ArrayList<>(converters);
            sorted.sort(Comparator.comparingInt(c -> c.getPhase().getOrder()));
            return new ConversionPipeline(List.copyOf(sorted));
        }
    }
}
