/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 * Request for executing an optimized query through the query planner.
 *
 * This request wraps a SearchSourceBuilder (DSL query) and target indices,
 * which will be processed through the optimization and physical planning pipeline.
 */
public class QSearchRequest extends ActionRequest implements IndicesRequest {

    private String[] indices = new String[0];
    private SearchSourceBuilder source;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpenAndForbidClosedIgnoreThrottled();

    /**
     * Constructs a new QSearchRequest.
     */
    public QSearchRequest() {}

    /**
     * Constructs a new QSearchRequest from a stream.
     *
     * @param in the stream to read from
     * @throws IOException if an I/O error occurs
     */
    public QSearchRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        if (in.readBoolean()) {
            source = new SearchSourceBuilder(in);
        }
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        if (source == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            source.writeTo(out);
        }
        indicesOptions.writeIndicesOptions(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (source == null) {
            validationException = new ActionRequestValidationException();
            validationException.addValidationError("search source is missing");
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    /**
     * Sets the indices to search.
     *
     * @param indices the indices to search
     * @return this request
     */
    public QSearchRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Sets the indices options.
     *
     * @param indicesOptions the indices options
     * @return this request
     */
    public QSearchRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * Gets the search source (DSL query).
     */
    public SearchSourceBuilder source() {
        return source;
    }

    /**
     * Sets the search source (DSL query).
     *
     * @param source the search source
     * @return this request
     */
    public QSearchRequest source(SearchSourceBuilder source) {
        this.source = source;
        return this;
    }

    @Override
    public String toString() {
        return "QSearchRequest{" + "indices=" + Arrays.toString(indices) + ", source=" + source + '}';
    }
}
