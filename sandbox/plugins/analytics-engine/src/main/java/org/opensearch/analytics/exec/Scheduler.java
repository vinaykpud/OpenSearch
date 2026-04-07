/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.TransportService;

import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * Coordinator-side orchestrator. Manages {@link PlanWalker} lifecycle via a
 * walker pool and gates per-node concurrency via {@link PendingExecutions}.
 *
 * <p>Uses {@link TransportService#sendRequest} to dispatch tasks to the target
 * data node, which triggers {@link TransportAnalyticsShardAction} on the remote
 * node. For local nodes, the transport layer short-circuits to a direct call.
 *
 * <p>Created in {@code createComponents()} with a {@code Supplier<TransportService>}
 * (since TransportService is not yet available at createComponents time) and
 * {@code maxConcurrentShardRequests}. NOT Guice-injected — must be created
 * before {@code DefaultPlanExecutor} so both can be wired in
 * {@code createComponents()} and {@code AnalyticsEngineService.setInstance()}
 * can be called.
 *
 * @opensearch.internal
 */
public class Scheduler {
    private final Supplier<TransportService> transportServiceSupplier;
    private final int maxConcurrentShardRequests;
    private final ConcurrentMap<String, PlanWalker> walkerPool = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, PendingExecutions> pendingExecutionsPerNode = new ConcurrentHashMap<>();

    public Scheduler(Supplier<TransportService> transportServiceSupplier, int maxConcurrentShardRequests) {
        this.transportServiceSupplier = transportServiceSupplier;
        this.maxConcurrentShardRequests = maxConcurrentShardRequests;
    }

    /**
     * Executes a PlanWalker asynchronously. Caches the walker during execution
     * and removes it on completion (success or failure).
     */
    public void execute(PlanWalker walker, ActionListener<Iterable<Object[]>> listener) {
        walkerPool.put(walker.getQueryId(), walker);
        walker.walk(this::dispatchTask, ActionListener.wrap(result -> {
            walkerPool.remove(walker.getQueryId());
            listener.onResponse(result);
        }, e -> {
            walkerPool.remove(walker.getQueryId());
            listener.onFailure(e);
        }));
    }

    /**
     * Dispatches a task to the target data node with per-node concurrency gating.
     * If permits are available for the target node, the task dispatches immediately
     * via {@link TransportService#sendRequest}. Otherwise it is queued and
     * dispatched when a permit is freed.
     */
    private void dispatchTask(
        FragmentExecutionRequest request,
        DiscoveryNode targetNode,
        ActionListener<FragmentExecutionResponse> listener
    ) {
        PendingExecutions pending = pendingExecutionsPerNode.computeIfAbsent(
            targetNode.getId(),
            n -> new PendingExecutions(maxConcurrentShardRequests)
        );

        pending.tryRun(() -> {
            transportServiceSupplier.get()
                .sendRequest(
                    targetNode,
                    AnalyticsShardAction.NAME,
                    request,
                    new ActionListenerResponseHandler<>(ActionListener.wrap(response -> {
                        try {
                            listener.onResponse(response);
                        } finally {
                            pending.finishAndRunNext();
                        }
                    }, e -> {
                        try {
                            listener.onFailure(e);
                        } finally {
                            pending.finishAndRunNext();
                        }
                    }), FragmentExecutionResponse::new)
                );
        });
    }

    /**
     * Permit-based concurrency queue per node. Same pattern as
     * {@code AbstractSearchAsyncAction.PendingExecutions} in OpenSearch core.
     *
     * <p>When permits are available, tasks run immediately. When all permits are
     * taken, tasks queue in a FIFO {@link ArrayDeque}. Completing a task releases
     * a permit and dequeues+runs the next waiting task.
     */
    static final class PendingExecutions {
        private final int permits;
        private int permitsTaken = 0;
        private final ArrayDeque<Runnable> queue = new ArrayDeque<>();

        PendingExecutions(int permits) {
            assert permits > 0 : "permits must be > 0: " + permits;
            this.permits = permits;
        }

        void tryRun(Runnable runnable) {
            Runnable toExecute = tryQueue(runnable);
            if (toExecute != null) {
                toExecute.run();
            }
        }

        void finishAndRunNext() {
            synchronized (this) {
                permitsTaken--;
                assert permitsTaken >= 0 : "illegal permits: " + permitsTaken;
            }
            tryRun(null);
        }

        private synchronized Runnable tryQueue(Runnable runnable) {
            Runnable toExecute = null;
            if (permitsTaken < permits) {
                permitsTaken++;
                toExecute = runnable;
                if (toExecute == null) {
                    toExecute = queue.poll();
                }
                if (toExecute == null) {
                    permitsTaken--;
                }
            } else if (runnable != null) {
                queue.add(runnable);
            }
            return toExecute;
        }
    }
}
