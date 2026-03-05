/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.queryplanner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;

/**
 * A {@link RelNodeExecutor} that logs the RelNode tree and returns empty rows.
 * Used as a placeholder until a real execution engine is available.
 */
public class LoggingRelNodeExecutor implements RelNodeExecutor {

    private static final Logger logger = LogManager.getLogger(LoggingRelNodeExecutor.class);

    /** Creates a new logging RelNode executor. */
    public LoggingRelNodeExecutor() {}

    @Override
    public void execute(RelNode relNode, ActionListener<Object[][]> listener) {
        RelMetadataQueryBase.THREAD_PROVIDERS.set(
            JaninoRelMetadataProvider.of(relNode.getCluster().getMetadataProvider())
        );
        relNode.getCluster().invalidateMetadataQuery();
        logger.info("Executing RelNode:\n{}", relNode.explain());
        listener.onResponse(new Object[0][]);
    }
}
