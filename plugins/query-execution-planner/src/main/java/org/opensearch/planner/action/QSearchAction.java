/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.action;

import org.opensearch.action.ActionType;

/**
 * Action type for executing optimized queries through the query planner.
 *
 * This action processes queries through:
 * 1. Query optimization using Apache Calcite
 * 2. Physical plan generation with engine assignment
 * 3. Hybrid execution coordinating Lucene and DataFusion
 */
public class QSearchAction extends ActionType<QSearchResponse> {

    /** Singleton instance of QSearchAction */
    public static final QSearchAction INSTANCE = new QSearchAction();

    /** Action name for transport layer */
    public static final String NAME = "indices:data/read/qsearch";

    private QSearchAction() {
        super(NAME, QSearchResponse::new);
    }
}
