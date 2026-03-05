/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.queryplanner;

import org.apache.calcite.rel.RelNode;
import org.opensearch.core.action.ActionListener;

/**
 * Adapter interface for executing a single Calcite {@link RelNode} asynchronously.
 * Implementations handle the actual execution (e.g., logging, forwarding to
 * a query engine) and deliver results via the {@link ActionListener} callback.
 */
public interface RelNodeExecutor {

    /**
     * Executes a single RelNode asynchronously.
     * Results are delivered via the listener callback.
     *
     * @param relNode  the Calcite relational node to execute
     * @param listener callback for the result rows or failure
     */
    void execute(RelNode relNode, ActionListener<Object[][]> listener);
}
