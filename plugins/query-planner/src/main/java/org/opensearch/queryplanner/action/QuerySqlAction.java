/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.action;

import org.opensearch.action.ActionType;

/**
 * Action for executing SQL queries via the query planner.
 * This action accepts SQL, parses it, optimizes the plan, and executes it.
 */
public class QuerySqlAction extends ActionType<QuerySqlResponse> {

    public static final QuerySqlAction INSTANCE = new QuerySqlAction();
    public static final String NAME = "indices:data/read/query_sql";

    private QuerySqlAction() {
        super(NAME, QuerySqlResponse::new);
    }
}
