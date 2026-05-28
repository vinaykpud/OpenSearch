/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;

import java.util.List;
import java.util.Map;

/**
 * One step in the post-Volcano transformation of a FINAL {@link OpenSearchAggregate}'s input
 * chain. Transformers are composed by {@code DistributedAggregateRewriter} — each must return
 * {@code current} unchanged when it has nothing to do, so the orchestrator can skip the
 * aggCall rebuild on a no-op pass.
 *
 * @opensearch.internal
 */
interface FinalInputTransformer {

    TransformResult transform(OpenSearchAggregate finalAgg, TransformResult current);

    /**
     * Carries the running input chain plus, when {@link LiteralProjectTransformer} fires, the
     * per-call indexes of any literal columns that were appended.
     */
    record TransformResult(RelNode finalInput, Map<Integer, List<Integer>> extraLiteralColIdxByCallIdx) {

        static TransformResult initial(RelNode finalInput) {
            return new TransformResult(finalInput, Map.of());
        }

        boolean changed(RelNode original) {
            return finalInput != original || !extraLiteralColIdxByCallIdx.isEmpty();
        }
    }
}
