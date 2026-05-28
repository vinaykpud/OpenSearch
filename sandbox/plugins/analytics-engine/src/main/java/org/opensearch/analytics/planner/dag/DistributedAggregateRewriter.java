/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.opensearch.analytics.planner.dag.FinalInputTransformer.TransformResult;
import org.opensearch.analytics.planner.rel.FinalAggCallBuilder;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;

import java.util.List;

/**
 * Post-Volcano orchestrator: runs each {@link FinalInputTransformer} over the FINAL aggregate's
 * input chain, then rebinds its aggCalls via {@link FinalAggCallBuilder} when a transformer
 * actually changed something. Add new post-Volcano concerns by appending to
 * {@link #TRANSFORMERS}.
 *
 * @opensearch.internal
 */
final class DistributedAggregateRewriter {

    private static final List<FinalInputTransformer> TRANSFORMERS = List.of(
        new ExchangeTypeOverrideTransformer(),
        new LiteralProjectTransformer()
    );

    private DistributedAggregateRewriter() {}

    static RelNode rewrite(OpenSearchAggregate finalAgg) {
        RelNode exchange = finalAgg.getInput();
        // Skip degenerate trees where the exchange has no child or the leaf below it isn't
        // a StageInputScan — transformers target the StageInputScan layer.
        if (exchange.getInputs().isEmpty()) return finalAgg;
        if (!(exchange.getInputs().get(0) instanceof OpenSearchStageInputScan)) return finalAgg;

        TransformResult result = TransformResult.initial(exchange);
        for (FinalInputTransformer transformer : TRANSFORMERS) {
            result = transformer.transform(finalAgg, result);
        }
        if (!result.changed(exchange)) return finalAgg;

        List<AggregateCall> rebuiltCalls = FinalAggCallBuilder.buildFinalCalls(
            finalAgg.getAggCallList(),
            finalAgg.getDecompositions(),
            finalAgg.getGroupSet().cardinality(),
            result.finalInput(),
            finalAgg.getGroupSet().isEmpty(),
            result.extraLiteralColIdxByCallIdx()
        );

        return OpenSearchAggregate.finalAfterRewrite(finalAgg, result.finalInput(), rebuiltCalls);
    }
}
