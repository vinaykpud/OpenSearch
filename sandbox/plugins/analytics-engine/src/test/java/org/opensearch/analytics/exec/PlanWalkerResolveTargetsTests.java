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
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PlanWalker} target resolution via walk() with a capturing TaskSubmitter.
 */
public class PlanWalkerResolveTargetsTests extends OpenSearchTestCase {

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

    private Map<String, AnalyticsSearchBackendPlugin> mockBackends(String... backendIds) {
        Map<String, AnalyticsSearchBackendPlugin> map = new HashMap<>();
        for (String id : backendIds) {
            AnalyticsSearchBackendPlugin backend = mock(AnalyticsSearchBackendPlugin.class);
            FragmentConvertor convertor = mock(FragmentConvertor.class);
            when(convertor.convertScanFragment(anyString(), any())).thenReturn(new byte[0]);
            when(convertor.convertShuffleReadFragment(anyString(), any())).thenReturn(new byte[0]);
            when(backend.getFragmentConvertor()).thenReturn(convertor);
            map.put(id, backend);
        }
        return map;
    }

    private OpenSearchTableScan buildTableScan(String tableName, List<String> viableBackends) {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("default", tableName));
        when(table.getRowType()).thenReturn(rowType);
        return new OpenSearchTableScan(cluster, RelTraitSet.createEmpty(), table, viableBackends, List.of());
    }

    private ClusterState buildMockClusterState(String tableName, int numShards) {
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        Index index = new Index(tableName, "_na_");
        when(indexMetadata.getIndex()).thenReturn(index);
        when(metadata.index(tableName)).thenReturn(indexMetadata);
        when(clusterState.metadata()).thenReturn(metadata);

        RoutingTable routingTable = mock(RoutingTable.class);
        IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);
        when(routingTable.index(index)).thenReturn(indexRoutingTable);
        when(clusterState.routingTable()).thenReturn(routingTable);

        Map<Integer, IndexShardRoutingTable> shardMap = new HashMap<>();
        for (int i = 0; i < numShards; i++) {
            IndexShardRoutingTable shardRoutingTable = mock(IndexShardRoutingTable.class);
            ShardRouting primaryShard = mock(ShardRouting.class);
            when(primaryShard.shardId()).thenReturn(new ShardId(index, i));
            when(primaryShard.currentNodeId()).thenReturn("node_" + i);
            when(shardRoutingTable.primaryShard()).thenReturn(primaryShard);
            when(indexRoutingTable.shard(i)).thenReturn(shardRoutingTable);
            shardMap.put(i, shardRoutingTable);
        }
        when(indexRoutingTable.shards()).thenReturn(shardMap);

        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        for (int i = 0; i < numShards; i++) {
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(discoveryNodes.get("node_" + i)).thenReturn(node);
        }
        when(clusterState.nodes()).thenReturn(discoveryNodes);

        return clusterState;
    }

    public void testLeafStageWithTableScanResolvesAllPrimaryShards() {
        int numShards = 3;
        ClusterState clusterState = buildMockClusterState("http_logs", numShards);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan);
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));
        QueryDAG dag = new QueryDAG("test-query", stage);

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        List<FragmentExecutionRequest> capturedRequests = new ArrayList<>();

        TaskSubmitter submitter = (request, node, listener) -> {
            capturedRequests.add(request);
            listener.onResponse(new FragmentExecutionResponse(List.of(), List.of()));
        };

        PlanWalker walker = new PlanWalker(dag, clusterState, mockBackends("lucene"));
        walker.walk(submitter, future);
        future.actionGet();

        assertEquals(numShards, capturedRequests.size());
        for (int i = 0; i < numShards; i++) {
            ShardId shardId = capturedRequests.get(i).getShardId();
            assertEquals("http_logs", shardId.getIndexName());
            assertEquals(i, shardId.id());
        }
    }

    public void testSingletonExchangeResolvesAllPrimaryShards() {
        int numShards = 3;
        ClusterState clusterState = buildMockClusterState("http_logs", numShards);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan);
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage childStage = new Stage(0, scan, List.of(), singletonExchange);
        childStage.setPlanAlternatives(List.of(plan));

        // Root stage with StageInputScan (coordinator-only)
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 0, rowType);
        StagePlan rootPlan = new StagePlan(stageInput);
        Stage rootStage = new Stage(1, stageInput, List.of(childStage), null);
        rootStage.setPlanAlternatives(List.of(rootPlan));

        QueryDAG dag = new QueryDAG("test-query", rootStage);

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        List<FragmentExecutionRequest> capturedRequests = new ArrayList<>();

        TaskSubmitter submitter = (request, node, listener) -> {
            capturedRequests.add(request);
            listener.onResponse(new FragmentExecutionResponse(List.of(), List.of()));
        };

        PlanWalker walker = new PlanWalker(dag, clusterState, mockBackends("lucene"));
        walker.walk(submitter, future);
        future.actionGet();

        // Only the child stage (SINGLETON with TableScan) should dispatch tasks.
        // The root stage (coordinator-only with StageInputScan) resolves empty targets.
        assertEquals(numShards, capturedRequests.size());
        for (int i = 0; i < numShards; i++) {
            ShardId shardId = capturedRequests.get(i).getShardId();
            assertEquals("http_logs", shardId.getIndexName());
            assertEquals(i, shardId.id());
        }
    }

    public void testCoordinatorOnlyStageResolvesEmpty() {
        ClusterState clusterState = mock(ClusterState.class);

        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 0, rowType);
        StagePlan plan = new StagePlan(stageInput);
        Stage stage = new Stage(1, stageInput, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));

        QueryDAG dag = new QueryDAG("test-query", stage);

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        List<FragmentExecutionRequest> capturedRequests = new ArrayList<>();

        TaskSubmitter submitter = (request, node, listener) -> {
            capturedRequests.add(request);
            listener.onResponse(new FragmentExecutionResponse(List.of(), List.of()));
        };

        PlanWalker walker = new PlanWalker(dag, clusterState, mockBackends("lucene"));
        walker.walk(submitter, future);
        future.actionGet();

        // Coordinator-only stage with StageInputScan (no TableScan) should dispatch zero tasks
        assertTrue(capturedRequests.isEmpty());
    }

    public void testHashDistributedThrows() {
        int numShards = 2;
        ClusterState clusterState = buildMockClusterState("http_logs", numShards);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan);
        ExchangeInfo hashExchange = new ExchangeInfo(RelDistribution.Type.HASH_DISTRIBUTED, null, List.of());
        Stage childStage = new Stage(0, scan, List.of(), hashExchange);
        childStage.setPlanAlternatives(List.of(plan));

        // Root stage with StageInputScan
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 0, rowType);
        StagePlan rootPlan = new StagePlan(stageInput);
        Stage rootStage = new Stage(1, stageInput, List.of(childStage), null);
        rootStage.setPlanAlternatives(List.of(rootPlan));

        QueryDAG dag = new QueryDAG("test-query", rootStage);

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        TaskSubmitter submitter = (request, node, listener) -> {
            listener.onResponse(new FragmentExecutionResponse(List.of(), List.of()));
        };

        PlanWalker walker = new PlanWalker(dag, clusterState, mockBackends("lucene"));
        walker.walk(submitter, future);

        RuntimeException ex = expectThrows(RuntimeException.class, future::actionGet);
        assertTrue(ex.getCause() instanceof UnsupportedOperationException || ex instanceof UnsupportedOperationException);
    }
}
