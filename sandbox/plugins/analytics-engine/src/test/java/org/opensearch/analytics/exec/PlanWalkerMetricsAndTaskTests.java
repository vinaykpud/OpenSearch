/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for StageMetrics integration in PlanWalker dispatch, Task API (fork, getParentTask,
 * Scheduler sendChildRequest/sendRequest routing), and existing behavior preservation with defaults.
 *
 * Validates: Requirements 4.2, 5.5, 5.6, 5.7, 6.8, 6.9, 6.10, 7.1
 */
@SuppressWarnings("unchecked")
public class PlanWalkerMetricsAndTaskTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RelOptCluster cluster;
    private RelDataType rowType;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        rowType = typeFactory.builder().add("field_0", SqlTypeName.VARCHAR).build();
    }

    private OpenSearchTableScan buildTableScan(String tableName, List<String> viableBackends) {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("default", tableName));
        when(table.getRowType()).thenReturn(rowType);
        return new OpenSearchTableScan(cluster, RelTraitSet.createEmpty(), table, viableBackends, List.of());
    }

    private ClusterService buildMockClusterService(String tableName, int numShards) {
        Index index = new Index(tableName, "_na_");

        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        for (int i = 0; i < numShards; i++) {
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(node.getId()).thenReturn("node_" + i);
            when(discoveryNodes.get("node_" + i)).thenReturn(node);
        }
        when(clusterState.nodes()).thenReturn(discoveryNodes);

        List<ShardIterator> iterators = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            ShardIterator shardIt = mock(ShardIterator.class);
            ShardRouting shard = mock(ShardRouting.class);
            when(shard.shardId()).thenReturn(new ShardId(index, i));
            when(shard.currentNodeId()).thenReturn("node_" + i);
            when(shardIt.nextOrNull()).thenReturn(shard);
            iterators.add(shardIt);
        }
        GroupShardsIterator<ShardIterator> groupIterator = new GroupShardsIterator<>(iterators);

        OperationRouting operationRouting = mock(OperationRouting.class);
        when(operationRouting.searchShards(any(), eq(new String[] { tableName }), isNull(), isNull())).thenReturn(groupIterator);

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.operationRouting()).thenReturn(operationRouting);

        return clusterService;
    }

    // ---- Task 11.2: StageMetrics integration tests ----

    /**
     * StageMetrics entry exists after dispatch with correct counters.
     * Builds a single-stage DAG with N shards, walks it, then uses reflection
     * to access the stageMetrics map. Asserts StageMetrics entry exists for the
     * stage, tasksCompleted == N, tasksFailed == 0, startTimeMs > 0, and
     * endTimeMs >= startTimeMs.
     *
     * Validates: Requirements 5.5, 5.6, 5.7
     */
    public void testDispatchStageCreatesStageMetrics() throws Exception {
        int numShards = 3;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan, "lucene");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));
        QueryDAG dag = new QueryDAG("test-query", stage);

        TaskSubmitter submitter = (request, node, listener) -> {
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] { "row_" + request.getShardId().id() });
            listener.onResponse(new FragmentExecutionResponse(List.of("field_0"), rows));
        };

        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(submitter, future);
        future.actionGet();

        // Use reflection to access stageMetrics map
        Field stageMetricsField = PlanWalker.class.getDeclaredField("stageMetrics");
        stageMetricsField.setAccessible(true);
        Map<Integer, StageMetrics> metricsMap = (Map<Integer, StageMetrics>) stageMetricsField.get(walker);

        StageMetrics metrics = metricsMap.get(0);
        assertNotNull("StageMetrics entry should exist for stage 0", metrics);
        assertEquals("tasksCompleted should equal number of shards", numShards, metrics.getTasksCompleted());
        assertEquals("tasksFailed should be 0", 0, metrics.getTasksFailed());
        assertTrue("startTimeMs should be > 0", metrics.getStartTimeMs() > 0);
        assertTrue("endTimeMs should be >= startTimeMs", metrics.getEndTimeMs() >= metrics.getStartTimeMs());
    }

    /**
     * tasksFailed incremented on shard failure.
     * Builds a single-stage DAG, makes one shard fail.
     * Asserts tasksFailed >= 1 in the StageMetrics.
     *
     * Validates: Requirements 5.6
     */
    public void testDispatchStageMetricsOnFailure() throws Exception {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan, "lucene");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));
        QueryDAG dag = new QueryDAG("test-query", stage);

        AtomicInteger callCount = new AtomicInteger(0);
        TaskSubmitter submitter = (request, node, listener) -> {
            int call = callCount.getAndIncrement();
            if (call == 0) {
                List<Object[]> rows = new ArrayList<>();
                rows.add(new Object[] { "ok" });
                listener.onResponse(new FragmentExecutionResponse(List.of("field_0"), rows));
            } else {
                listener.onFailure(new RuntimeException("shard failed"));
            }
        };

        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(submitter, future);

        // Walk should fail
        expectThrows(RuntimeException.class, future::actionGet);

        // Use reflection to access stageMetrics map
        Field stageMetricsField = PlanWalker.class.getDeclaredField("stageMetrics");
        stageMetricsField.setAccessible(true);
        Map<Integer, StageMetrics> metricsMap = (Map<Integer, StageMetrics>) stageMetricsField.get(walker);

        StageMetrics metrics = metricsMap.get(0);
        assertNotNull("StageMetrics entry should exist for stage 0", metrics);
        assertTrue("tasksFailed should be >= 1", metrics.getTasksFailed() >= 1);
        assertTrue("startTimeMs should be > 0", metrics.getStartTimeMs() > 0);
        assertTrue("endTimeMs should be >= startTimeMs", metrics.getEndTimeMs() >= metrics.getStartTimeMs());
    }

    // ---- Task 11.3: Task API and executor tests ----

    /**
     * fork() executes runnable on provided executor.
     * Creates PlanWalker with a capturing executor, calls fork(), asserts the
     * runnable was executed.
     *
     * Validates: Requirements 4.2
     */
    public void testForkDelegatesToExecutor() {
        ClusterService clusterService = mock(ClusterService.class);

        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 0, rowType, List.of());
        Stage stage = new Stage(0, stageInput, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(stageInput, "lucene")));
        QueryDAG dag = new QueryDAG("test-query", stage);

        // Capturing executor that records submitted runnables
        AtomicReference<Runnable> captured = new AtomicReference<>();
        PlanWalker walker = new PlanWalker(dag, clusterService, r -> {
            captured.set(r);
            r.run();
        }, null);

        AtomicInteger counter = new AtomicInteger(0);
        walker.fork(counter::incrementAndGet);

        assertNotNull("Executor should have received the runnable", captured.get());
        assertEquals("Runnable should have been executed", 1, counter.get());
    }

    /**
     * getParentTask() returns AnalyticsQueryTask passed to constructor.
     *
     * Validates: Requirements 6.9
     */
    public void testPlanWalkerGetParentTask() {
        ClusterService clusterService = mock(ClusterService.class);

        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 0, rowType, List.of());
        Stage stage = new Stage(0, stageInput, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(stageInput, "lucene")));
        QueryDAG dag = new QueryDAG("test-query", stage);

        AnalyticsQueryTask queryTask = new AnalyticsQueryTask(
            1L,
            "transport",
            "analytics_query",
            "test-query",
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, queryTask);
        assertSame("getParentTask() should return the task passed to constructor", queryTask, walker.getParentTask());
    }

    /**
     * sendChildRequest called with AnalyticsQueryTask when parentTask is non-null.
     * Creates Scheduler with mock TransportService, creates PlanWalker with non-null
     * AnalyticsQueryTask. Executes and verifies sendChildRequest(Connection, ...) was
     * called (not the 4-arg sendRequest). The 6-arg sendChildRequest(DiscoveryNode, ...)
     * is final, so we mock getConnection() and stub the non-final Connection overload.
     *
     * Validates: Requirements 6.10
     */
    public void testSchedulerUsesSendChildRequestWhenParentTaskPresent() throws Exception {
        TransportService transportService = mock(TransportService.class);
        org.opensearch.transport.Transport.Connection mockConnection = mock(org.opensearch.transport.Transport.Connection.class);
        Scheduler scheduler = new Scheduler(transportService, 5);

        // Mock getConnection to return our mock connection
        when(transportService.getConnection(any(DiscoveryNode.class))).thenReturn(mockConnection);

        // Mock the non-final sendChildRequest(Connection, ...) to respond immediately
        doAnswer(invocation -> {
            org.opensearch.transport.TransportResponseHandler<FragmentExecutionResponse> handler = invocation.getArgument(5);
            handler.handleResponse(new FragmentExecutionResponse(List.of("field_0"), List.of()));
            return null;
        }).when(transportService)
            .sendChildRequest(
                any(org.opensearch.transport.Transport.Connection.class),
                anyString(),
                any(TransportRequest.class),
                any(Task.class),
                any(TransportRequestOptions.class),
                any(org.opensearch.transport.TransportResponseHandler.class)
            );

        ClusterService clusterService = buildMockClusterService("http_logs", 1);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan, "lucene");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));
        QueryDAG dag = new QueryDAG("test-query", stage);

        AnalyticsQueryTask queryTask = new AnalyticsQueryTask(
            1L,
            "transport",
            "analytics_query",
            "test-query",
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, queryTask);
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        scheduler.execute(walker, future);
        future.actionGet();

        // Verify sendChildRequest(Connection, ...) was called
        verify(transportService).sendChildRequest(
            eq(mockConnection),
            eq(AnalyticsShardAction.NAME),
            any(TransportRequest.class),
            eq(queryTask),
            eq(TransportRequestOptions.EMPTY),
            any(org.opensearch.transport.TransportResponseHandler.class)
        );

        // Verify the 4-arg sendRequest(DiscoveryNode, ...) was NOT called
        verify(transportService, never()).sendRequest(
            any(DiscoveryNode.class),
            anyString(),
            any(TransportRequest.class),
            any(org.opensearch.transport.TransportResponseHandler.class)
        );
    }

    /**
     * sendRequest called when parentTask is null (test fallback).
     * Creates Scheduler with mock TransportService, creates PlanWalker with null parentTask.
     * Executes and verifies sendRequest was called (not sendChildRequest).
     *
     * Validates: Requirements 6.10
     */
    public void testSchedulerUsesSendRequestWhenParentTaskNull() throws Exception {
        TransportService transportService = mock(TransportService.class);
        Scheduler scheduler = new Scheduler(transportService, 5);

        // Mock sendRequest to respond immediately
        doAnswer(invocation -> {
            org.opensearch.transport.TransportResponseHandler<FragmentExecutionResponse> handler = invocation.getArgument(3);
            handler.handleResponse(new FragmentExecutionResponse(List.of("field_0"), List.of()));
            return null;
        }).when(transportService)
            .sendRequest(
                any(DiscoveryNode.class),
                anyString(),
                any(TransportRequest.class),
                any(org.opensearch.transport.TransportResponseHandler.class)
            );

        ClusterService clusterService = buildMockClusterService("http_logs", 1);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan, "lucene");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));
        QueryDAG dag = new QueryDAG("test-query", stage);

        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        scheduler.execute(walker, future);
        future.actionGet();

        // Verify sendRequest was called
        verify(transportService).sendRequest(
            any(DiscoveryNode.class),
            eq(AnalyticsShardAction.NAME),
            any(TransportRequest.class),
            any(org.opensearch.transport.TransportResponseHandler.class)
        );

        // Verify sendChildRequest(Connection, ...) was NOT called
        verify(transportService, never()).sendChildRequest(
            any(org.opensearch.transport.Transport.Connection.class),
            anyString(),
            any(TransportRequest.class),
            any(Task.class),
            any(TransportRequestOptions.class),
            any(org.opensearch.transport.TransportResponseHandler.class)
        );
    }

    // ---- Task 11.4: Existing behavior preservation test ----

    /**
     * All defaults produce identical results to current implementation.
     * Builds a single-stage DAG with 2 shards, all Stage fields at defaults
     * (IDENTITY, DISPATCH_ALL, false). Walks it, verifies results are correct
     * (same as before the changes).
     *
     * Validates: Requirements 7.1
     */
    public void testExistingBehaviorPreservedWithDefaults() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan, "lucene");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));

        // Verify all defaults are in place
        assertSame("ShardFilterPhase should default to IDENTITY", ShardFilterPhase.IDENTITY, stage.getShardFilterPhase());
        assertSame("TerminationDecider should default to DISPATCH_ALL", TerminationDecider.DISPATCH_ALL, stage.getTerminationDecider());
        assertEquals("parallelChildren should default to false", false, stage.isParallelChildren());

        QueryDAG dag = new QueryDAG("test-query", stage);

        TaskSubmitter submitter = (request, node, listener) -> {
            int shardIdx = request.getShardId().id();
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] { "value_" + shardIdx });
            listener.onResponse(new FragmentExecutionResponse(List.of("field_0"), rows));
        };

        // Use Runnable::run as executor (synchronous, same as existing test behavior)
        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(submitter, future);

        Iterable<Object[]> result = future.actionGet();
        List<Object[]> resultList = new ArrayList<>();
        result.forEach(resultList::add);

        // 2 shards × 1 row each = 2 total rows
        assertEquals("Should have 2 rows from 2 shards", 2, resultList.size());

        // Verify all shard values are present
        List<String> values = new ArrayList<>();
        for (Object[] row : resultList) {
            values.add((String) row[0]);
        }
        assertTrue("Should contain value_0", values.contains("value_0"));
        assertTrue("Should contain value_1", values.contains("value_1"));
    }
}
