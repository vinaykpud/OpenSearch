/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.lucene.LuceneSearchEnginePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;
import org.junit.After;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * End-to-end integration test for the analytics shard dispatch pipeline.
 *
 * <p>Uses {@link MockTransportService} to intercept {@link AnalyticsShardAction}
 * requests at the transport layer and return mock {@link FragmentExecutionResponse}
 * results. This tests the full coordinator path: DefaultPlanExecutor to PlannerImpl
 * to Scheduler to PlanWalker to transport dispatch to mock response to sink to result.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class AnalyticsShardDispatchIT extends OpenSearchIntegTestCase {

    private static final Logger logger = LogManager.getLogger(AnalyticsShardDispatchIT.class);
    private static final String TEST_INDEX = "test_index";
    private String coordNodeName;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockTransportService.TestPlugin.class, TestPPLPlugin.class, FlightStreamPlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            new PluginInfo(
                AnalyticsPlugin.class.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                AnalyticsPlugin.class.getName(),
                null,
                Collections.emptyList(),
                false
            ),
            new PluginInfo(
                // TODO: Mock this - this dependency shouldn't exist in this IT.
                LuceneSearchEnginePlugin.class.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                LuceneSearchEnginePlugin.class.getName(),
                null,
                List.of(AnalyticsPlugin.class.getName()),
                false
            )
        );
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Start a coord-only node so PPL queries go over the wire to data nodes,
        // where MockTransportService can intercept the shard action requests
        coordNodeName = internalCluster().startNode(Settings.builder().put("node.data", false).put("node.master", false).build());
    }

    @After
    public void cleanup() {
        for (TransportService ts : internalCluster().getInstances(TransportService.class)) {
            ((MockTransportService) ts).clearAllRules();
        }
        if (indexExists(TEST_INDEX)) {
            client().admin().indices().prepareDelete(TEST_INDEX).get();
        }
    }

    /**
     * Returns a client connected to the coord-only node.
     * Queries sent from here go over the wire to data nodes,
     * where MockTransportService can intercept them.
     */
    private org.opensearch.transport.client.Client coordClient() {
        return internalCluster().client(coordNodeName);
    }

    public void testSingleShardQueryReturnsMockedResults() throws Exception {
        prepareCreate(TEST_INDEX).setSettings(
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        ).setMapping("status", "type=keyword").get();
        client().prepareIndex(TEST_INDEX).setId("1").setSource("status", "active").get();
        refresh(TEST_INDEX);
        ensureGreen(TEST_INDEX);

        // Register mock on the data node — intercepts inbound shard action requests
        String dataNodeName = internalCluster().getDataNodeNames().iterator().next();
        MockTransportService dataNodeTransport = (MockTransportService) internalCluster().getInstance(TransportService.class, dataNodeName);
        dataNodeTransport.addRequestHandlingBehavior(
            AnalyticsShardAction.NAME,
            (handler, request, channel, task) -> channel.sendResponse(
                new FragmentExecutionResponse(List.of("status"), List.<Object[]>of(new Object[] { "mocked_active" }))
            )
        );

        // Send query from coord-only node → goes over wire → mock intercepts on data node
        PPLRequest pplRequest = new PPLRequest("source = test_index | fields status");
        PPLResponse response = coordClient().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

        assertNotNull("PPLResponse should not be null", response);
        assertNotNull("Columns should not be null", response.getColumns());
        assertFalse("Columns should not be empty", response.getColumns().isEmpty());
        assertEquals("Should have 1 column", 1, response.getColumns().size());
        assertTrue("Columns should contain 'status'", response.getColumns().contains("status"));
        assertFalse("Should have at least 1 row", response.getRows().isEmpty());
    }

    public void testMockInterceptsAndReturnsCustomData() throws Exception {
        prepareCreate(TEST_INDEX).setSettings(
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        ).setMapping("status", "type=keyword").get();
        client().prepareIndex(TEST_INDEX).setId("1").setSource("status", "active").get();
        refresh(TEST_INDEX);
        ensureGreen(TEST_INDEX);

        // Register mock that returns 3 rows (proving mock overrides real shard execution)
        String dataNodeName = internalCluster().getDataNodeNames().iterator().next();
        MockTransportService dataNodeTransport = (MockTransportService) internalCluster().getInstance(TransportService.class, dataNodeName);
        dataNodeTransport.addRequestHandlingBehavior(
            AnalyticsShardAction.NAME,
            (handler, request, channel, task) -> channel.sendResponse(
                new FragmentExecutionResponse(
                    List.of("status"),
                    List.of(new Object[] { "mocked_1" }, new Object[] { "mocked_2" }, new Object[] { "mocked_3" })
                )
            )
        );

        PPLRequest pplRequest = new PPLRequest("source = test_index | fields status");
        PPLResponse response = coordClient().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

        assertNotNull("PPLResponse should not be null", response);
        assertNotNull("Columns should not be null", response.getColumns());
        assertEquals("Should have 1 column", 1, response.getColumns().size());
        // Mock returned 3 rows even though index has 1 doc — proves mock intercepted
        assertEquals("Should have 3 rows from mock", 3, response.getRows().size());
    }

    /**
     * Multi-shard query with aggregate that triggers aggregate splitting (partial + final)
     * and exchange insertion. This produces a 2-stage DAG with actual aggregate operators.
     * Expected to fail until multi-stage execution is wired — we just want to see the plan.
     */
    public void testMultiStageAggregateWithSumPlanOutput() throws Exception {
        String multiStageIndex = "multi_stage_agg_test";
        prepareCreate(multiStageIndex).setSettings(
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        ).setMapping("status", "type=keyword", "amount", "type=long").get();
        client().prepareIndex(multiStageIndex).setId("1").setSource("status", "active", "amount", 100).get();
        client().prepareIndex(multiStageIndex).setId("2").setSource("status", "active", "amount", 200).get();
        client().prepareIndex(multiStageIndex).setId("3").setSource("status", "inactive", "amount", 300).get();
        refresh(multiStageIndex);
        ensureGreen(multiStageIndex);

        // Register mock on data node
        String dataNodeName = internalCluster().getDataNodeNames().iterator().next();
        MockTransportService dataNodeTransport = (MockTransportService) internalCluster().getInstance(TransportService.class, dataNodeName);
        dataNodeTransport.addRequestHandlingBehavior(
            AnalyticsShardAction.NAME,
            (handler, request, channel, task) -> channel.sendResponse(
                new FragmentExecutionResponse(
                    List.of("status", "total"),
                    List.of(new Object[] { "active", 150L }, new Object[] { "inactive", 300L })
                )
            )
        );

        // SUM(amount) GROUP BY status — triggers aggregate split into partial + final:
        // Stage 0 [SINGLETON]: PartialAggregate(SUM) → Project → TableScan → dispatched to 3 shards
        // Stage 1 [root]: Project → FinalAggregate(SUM) → ExchangeReducer → StageInputScan(0)
        // Stage 0's mocked responses feed root sink. SimpleExchangeSink collects rows without
        // final reduction, so result = 3 shards × 2 rows each = 6 partial aggregate rows.
        PPLRequest pplRequest = new PPLRequest("source = " + multiStageIndex + " | stats sum(amount) as total by status");
        try {
            PPLResponse response = coordClient().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

            assertNotNull("Response should not be null", response);
            assertTrue("Columns should contain 'total'", response.getColumns().contains("total"));
            assertTrue("Columns should contain 'status'", response.getColumns().contains("status"));
            // 3 shards × 2 rows each = 6 partial aggregate rows (no final reduction in SimpleExchangeSink)
            assertEquals("Should have 6 rows (2 per shard × 3 shards)", 6, response.getRows().size());
        } finally {
            client().admin().indices().prepareDelete(multiStageIndex).get();
        }
    }

    /**
     * Multi-shard scan+project query that triggers SINGLETON exchange insertion
     * due to distribution mismatch (RANDOM shards → SINGLETON root).
     * Logs the QueryDAG output for inspection.
     */
    public void testMultiStageProjectPlanOutput() throws Exception {
        String multiStageIndex = "multi_stage_test";
        prepareCreate(multiStageIndex).setSettings(
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        ).setMapping("status", "type=keyword", "amount", "type=long").get();
        client().prepareIndex(multiStageIndex).setId("1").setSource("status", "active", "amount", 100).get();
        client().prepareIndex(multiStageIndex).setId("2").setSource("status", "active", "amount", 200).get();
        client().prepareIndex(multiStageIndex).setId("3").setSource("status", "inactive", "amount", 300).get();
        refresh(multiStageIndex);
        ensureGreen(multiStageIndex);

        // Register mock on data node to return partial aggregate results per shard
        String dataNodeName = internalCluster().getDataNodeNames().iterator().next();
        MockTransportService dataNodeTransport = (MockTransportService) internalCluster().getInstance(TransportService.class, dataNodeName);
        AtomicInteger counter = new AtomicInteger(0);
        dataNodeTransport.addRequestHandlingBehavior(AnalyticsShardAction.NAME, (handler, request, channel, task) -> {
            int shardNum = counter.getAndIncrement();
            channel.sendResponse(
                new FragmentExecutionResponse(
                    List.of("status", "count"),
                    List.of(new Object[] { "active", (long) (shardNum + 1) }, new Object[] { "inactive", (long) (shardNum * 2) })
                )
            );
        });

        // stats count() by status → triggers aggregate split:
        // Stage 0 [SINGLETON]: PartialAggregate(COUNT) → Project → TableScan → dispatched to 3 shards
        // Stage 1 [root]: Project → FinalAggregate(COUNT) → ExchangeReducer → StageInputScan(0)
        // Stage 0's mocked responses feed root sink. 3 shards × 2 rows each = 6 rows.
        PPLRequest pplRequest = new PPLRequest("source = " + multiStageIndex + " | stats count() as cnt by status");
        try {
            PPLResponse response = coordClient().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

            assertNotNull("Response should not be null", response);
            assertNotNull("Columns should not be null", response.getColumns());
            assertFalse("Columns should not be empty", response.getColumns().isEmpty());
            // 3 shards × 2 rows each = 6 partial aggregate rows
            assertEquals("Should have 6 rows (2 per shard × 3 shards)", 6, response.getRows().size());
        } finally {
            client().admin().indices().prepareDelete(multiStageIndex).get();
        }
    }

    public void testShardFailurePropagates() throws Exception {
        prepareCreate(TEST_INDEX).setSettings(
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        ).setMapping("status", "type=keyword").get();
        client().prepareIndex(TEST_INDEX).setId("1").setSource("status", "active").get();
        refresh(TEST_INDEX);
        ensureGreen(TEST_INDEX);

        // Register mock that returns an error
        String dataNodeName = internalCluster().getDataNodeNames().iterator().next();
        MockTransportService dataNodeTransport = (MockTransportService) internalCluster().getInstance(TransportService.class, dataNodeName);
        dataNodeTransport.addRequestHandlingBehavior(
            AnalyticsShardAction.NAME,
            (handler, request, channel, task) -> channel.sendResponse(new RuntimeException("shard failed"))
        );

        PPLRequest pplRequest = new PPLRequest("source = test_index | fields status");
        Exception exception = expectThrows(
            Exception.class,
            () -> coordClient().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet()
        );
        assertNotNull("Exception should not be null", exception);
        // Verify the exception chain contains our injected failure message somewhere
        Throwable cause = exception;
        boolean found = false;
        while (cause != null) {
            if (cause.getMessage() != null && cause.getMessage().contains("shard failed")) {
                found = true;
                break;
            }
            cause = cause.getCause();
        }
        assertTrue("Exception chain should contain 'shard failed' somewhere", found);
    }
}
