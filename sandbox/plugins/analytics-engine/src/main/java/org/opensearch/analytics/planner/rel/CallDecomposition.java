/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.opensearch.analytics.spi.AggregateFunction.IntermediateField;

/**
 * Per-call decomposition decision produced by {@link FinalAggCallBuilder#classify} and read
 * by post-Volcano transformers. Either carries an {@link IntermediateField} describing how
 * PARTIAL emits state and how FINAL merges it, or is the {@link #PASS_THROUGH} marker for
 * aggregates with no decomposition (FINAL re-uses the original function).
 *
 * @opensearch.internal
 */
public record CallDecomposition(IntermediateField field) {

    /** Marker for aggregates without an SPI-declared decomposition. */
    public static final CallDecomposition PASS_THROUGH = new CallDecomposition(null);

    public static CallDecomposition of(IntermediateField field) {
        return new CallDecomposition(field);
    }

    public boolean isDecomposed() {
        return field != null;
    }
}
