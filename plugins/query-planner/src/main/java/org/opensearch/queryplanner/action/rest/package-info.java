/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * REST API layer for the Query Planner plugin.
 *
 * <p>Exposes SQL query endpoint to external clients via HTTP.
 *
 * <h2>Key Classes:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.action.rest.RestQuerySqlAction} - REST handler for
 *       {@code POST /_sql}. Parses JSON body, creates QuerySqlRequest, forwards to
 *       transport layer.</li>
 * </ul>
 */
package org.opensearch.queryplanner.action.rest;
