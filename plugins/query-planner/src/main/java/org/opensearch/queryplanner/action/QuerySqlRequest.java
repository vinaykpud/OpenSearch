/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Request to execute a SQL query via the query planner.
 */
public class QuerySqlRequest extends ActionRequest {

    private final String sql;

    /**
     * Create a SQL query request.
     *
     * @param sql The SQL query to execute
     */
    public QuerySqlRequest(String sql) {
        this.sql = sql;
    }

    /**
     * Deserialize from stream.
     */
    public QuerySqlRequest(StreamInput in) throws IOException {
        super(in);
        this.sql = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sql);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sql == null || sql.trim().isEmpty()) {
            validationException = addValidationError("'sql' is required", validationException);
        }
        return validationException;
    }

    public String getSql() {
        return sql;
    }
}
