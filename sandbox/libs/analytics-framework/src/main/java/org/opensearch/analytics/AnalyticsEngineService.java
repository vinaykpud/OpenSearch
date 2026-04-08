/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.common.SetOnce;

/**
 * Static singleton providing the analytics engine's executor and context
 * to front-end plugins (PPL, SQL) that live in separate classloaders.
 *
 * <p>Set once by {@code DefaultPlanExecutor}'s Guice-injected constructor
 * at node startup. Consumed by transport actions in front-end plugins via
 * {@link #getInstance()}. Uses {@link SetOnce} to guarantee single
 * initialization — a second call to {@link #setInstance} throws
 * {@link IllegalStateException}.
 *
 * @opensearch.internal
 */
public class AnalyticsEngineService {

    private static final SetOnce<AnalyticsEngineService> INSTANCE = new SetOnce<>();

    private final EngineContext engineContext;
    private final QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor;

    public AnalyticsEngineService(EngineContext engineContext,
                                  QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor) {
        this.engineContext = engineContext;
        this.planExecutor = planExecutor;
    }

    public static void setInstance(AnalyticsEngineService instance) {
        INSTANCE.set(instance);
    }

    public static AnalyticsEngineService getInstance() {
        return INSTANCE.get();
    }

    public EngineContext getEngineContext() {
        return engineContext;
    }

    public QueryPlanExecutor<RelNode, Iterable<Object[]>> getPlanExecutor() {
        return planExecutor;
    }
}
