/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.junit.After;
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

    private static final String TEST_INDEX = "test_index";
    private String coordNodeName;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            MockTransportService.TestPlugin.class,
            TestPPLPlugin.class,
            FlightStreamPlugin.class
        );
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            new PluginInfo(
                AnalyticsPlugin.class.getName(), "classpath plugin", "NA",
                Version.CURRENT, "1.8", AnalyticsPlugin.class.getName(),
                null, Collections.emptyList(), false
            ),
            new PluginInfo(
                // TODO: Mock this - this dependency shouldn't exist in this IT.
                LuceneSearchEnginePlugin.class.getName(), "classpath plugin", "NA",
                Version.CURRENT, "1.8", LuceneSearchEnginePlugin.class.getName(),
                null, List.of(AnalyticsPlugin.class.getName()), false
            )
        );
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Start a coord-only node so PPL queries go over the wire to data nodes,
        // where MockTransportService can intercept the shard action requests
        coordNodeName = internalCluster().startNode(
            Settings.builder()
                .put("node.data", false)
                .put("node.master", false)
                .build()
        );
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
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        ).setMapping("status", "type=keyword").get();
        client().prepareIndex(TEST_INDEX).setId("1").setSource("status", "active").get();
        refresh(TEST_INDEX);
        ensureGreen(TEST_INDEX);

        // Register mock on the data node — intercepts inbound shard action requests
        String dataNodeName = internalCluster().getDataNodeNames().iterator().next();
        MockTransportService dataNodeTransport = (MockTransportService) internalCluster()
            .getInstance(TransportService.class, dataNodeName);
        dataNodeTransport.addRequestHandlingBehavior(
            AnalyticsShardAction.NAME,
            (handler, request, channel, task) -> channel.sendResponse(
                new FragmentExecutionResponse(
                    List.of("status"),
                    List.<Object[]>of(new Object[] { "mocked_active" })
                )
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
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        ).setMapping("status", "type=keyword").get();
        client().prepareIndex(TEST_INDEX).setId("1").setSource("status", "active").get();
        refresh(TEST_INDEX);
        ensureGreen(TEST_INDEX);

        // Register mock that returns 3 rows (proving mock overrides real shard execution)
        String dataNodeName = internalCluster().getDataNodeNames().iterator().next();
        MockTransportService dataNodeTransport = (MockTransportService) internalCluster()
            .getInstance(TransportService.class, dataNodeName);
        dataNodeTransport.addRequestHandlingBehavior(
            AnalyticsShardAction.NAME,
            (handler, request, channel, task) -> channel.sendResponse(
                new FragmentExecutionResponse(
                    List.of("status"),
                    List.of(
                        new Object[] { "mocked_1" },
                        new Object[] { "mocked_2" },
                        new Object[] { "mocked_3" }
                    )
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

    public void testShardFailurePropagates() throws Exception {
        prepareCreate(TEST_INDEX).setSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        ).setMapping("status", "type=keyword").get();
        client().prepareIndex(TEST_INDEX).setId("1").setSource("status", "active").get();
        refresh(TEST_INDEX);
        ensureGreen(TEST_INDEX);

        // Register mock that returns an error
        String dataNodeName = internalCluster().getDataNodeNames().iterator().next();
        MockTransportService dataNodeTransport = (MockTransportService) internalCluster()
            .getInstance(TransportService.class, dataNodeName);
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
