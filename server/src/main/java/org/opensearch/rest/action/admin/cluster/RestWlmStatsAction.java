/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.wlm.WlmStatsRequest;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Transport action to get Workload Management stats
 *
 * @opensearch.experimental
 */
public class RestWlmStatsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "_wlm/stats"),
                new Route(GET, "_wlm/{nodeId}/stats"),
                new Route(GET, "_wlm/stats/{workloadGroupId}"),
                new Route(GET, "_wlm/{nodeId}/stats/{workloadGroupId}")
            )
        );
    }

    @Override
    public String getName() {
        return "wlm_stats_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        Set<String> workloadGroupIds = Strings.tokenizeByCommaToSet(request.param("workloadGroupId", "_all"));
        Boolean breach = request.hasParam("breach") ? Boolean.parseBoolean(request.param("boolean")) : null;
        WlmStatsRequest wlmStatsRequest = new WlmStatsRequest(nodesIds, workloadGroupIds, breach);
        return channel -> client.admin().cluster().wlmStats(wlmStatsRequest, new RestActions.NodesResponseRestListener<>(channel));
    }
}
