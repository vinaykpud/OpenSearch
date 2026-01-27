/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.functions;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Utility class containing custom Calcite functions for OpenSearch-specific operations.
 * 
 * This class defines custom SQL functions that represent OpenSearch-specific query
 * capabilities that don't have direct Calcite equivalents.
 */
public class OpenSearchFunctions {

    /**
     * MATCH_QUERY function for full-text search.
     * 
     * Signature: MATCH_QUERY(field, text, operator)
     * - field: The field to search in
     * - text: The search text
     * - operator: The match operator (AND/OR)
     * 
     * Returns: BOOLEAN indicating if the document matches
     * 
     * This function represents OpenSearch's match query, which performs full-text
     * search with analysis and relevance scoring.
     */
    public static final SqlFunction MATCH_QUERY = new SqlFunction(
        "MATCH_QUERY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.STRING_STRING_STRING,  // field, text, operator
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private OpenSearchFunctions() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }
}
