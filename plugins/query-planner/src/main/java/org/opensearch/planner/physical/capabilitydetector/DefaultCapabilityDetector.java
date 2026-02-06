/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical.capabilitydetector;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.planner.physical.operators.ExecutionEngine;

import java.util.HashSet;
import java.util.Set;

/**
 * Default implementation of capability detection for execution engine assignment.
 *
 * <p>This implementation uses simple heuristics to determine which engine
 * should execute each operation:
 *
 * <p><b>Lucene Capabilities:</b>
 * <ul>
 *   <li>Table scans (always)</li>
 *   <li>Simple filters: =, !=, &lt;, &lt;=, &gt;, &gt;=, AND, OR, NOT</li>
 *   <li>Simple projections: field references only (no expressions)</li>
 * </ul>
 *
 * <p><b>DataFusion Capabilities:</b>
 * <ul>
 *   <li>All aggregations (GROUP BY, COUNT, SUM, AVG, etc.)</li>
 *   <li>All joins (INNER, LEFT, RIGHT, FULL)</li>
 *   <li>Complex filters (expressions, functions)</li>
 *   <li>Complex projections (computed columns)</li>
 *   <li>Sorting</li>
 * </ul>
 */
public class DefaultCapabilityDetector implements CapabilityDetector {

    // SQL operators that Lucene can handle
    private static final Set<SqlKind> LUCENE_SUPPORTED_OPERATORS = new HashSet<>();

    static {
        // Comparison operators
        LUCENE_SUPPORTED_OPERATORS.add(SqlKind.EQUALS);
        LUCENE_SUPPORTED_OPERATORS.add(SqlKind.NOT_EQUALS);
        LUCENE_SUPPORTED_OPERATORS.add(SqlKind.LESS_THAN);
        LUCENE_SUPPORTED_OPERATORS.add(SqlKind.LESS_THAN_OR_EQUAL);
        LUCENE_SUPPORTED_OPERATORS.add(SqlKind.GREATER_THAN);
        LUCENE_SUPPORTED_OPERATORS.add(SqlKind.GREATER_THAN_OR_EQUAL);

        // Boolean operators
        LUCENE_SUPPORTED_OPERATORS.add(SqlKind.AND);
        LUCENE_SUPPORTED_OPERATORS.add(SqlKind.OR);
        LUCENE_SUPPORTED_OPERATORS.add(SqlKind.NOT);

        // Other operators
        LUCENE_SUPPORTED_OPERATORS.add(SqlKind.IS_NULL);
        LUCENE_SUPPORTED_OPERATORS.add(SqlKind.IS_NOT_NULL);
        LUCENE_SUPPORTED_OPERATORS.add(SqlKind.LIKE);
    }

    @Override
    public ExecutionEngine detectEngine(RelNode operator) {
        if (operator instanceof TableScan) {
            return ExecutionEngine.LUCENE;
        } else if (operator instanceof Filter) {
            return canLuceneExecuteFilter((Filter) operator) ? ExecutionEngine.LUCENE : ExecutionEngine.DATAFUSION;
        } else if (operator instanceof Project) {
            return canLuceneExecuteProject((Project) operator) ? ExecutionEngine.LUCENE : ExecutionEngine.DATAFUSION;
        } else if (operator instanceof Aggregate) {
            return ExecutionEngine.DATAFUSION;
        } else if (operator instanceof Join) {
            return ExecutionEngine.DATAFUSION;
        } else if (operator instanceof Sort) {
            return ExecutionEngine.DATAFUSION;
        } else {
            // Default to DataFusion for unknown operators
            return ExecutionEngine.DATAFUSION;
        }
    }

    @Override
    public boolean canLuceneExecuteFilter(Filter filter) {
        // Check if the filter condition can be executed by Lucene
        RexNode condition = filter.getCondition();
        return canLuceneExecuteExpression(condition);
    }

    @Override
    public boolean canLuceneExecuteProject(Project project) {
        // Lucene can only handle simple field projections (no expressions)
        for (RexNode expr : project.getProjects()) {
            if (!isSimpleFieldReference(expr)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean canLuceneExecuteExpression(RexNode expression) {
        if (expression instanceof RexInputRef) {
            // Simple field reference - Lucene can handle
            return true;
        } else if (expression instanceof RexLiteral) {
            // Literal value - Lucene can handle
            return true;
        } else if (expression instanceof RexCall) {
            RexCall call = (RexCall) expression;
            SqlKind kind = call.getKind();

            // Check if operator is supported
            if (!LUCENE_SUPPORTED_OPERATORS.contains(kind)) {
                return false;
            }

            // Recursively check operands
            for (RexNode operand : call.getOperands()) {
                if (!canLuceneExecuteExpression(operand)) {
                    return false;
                }
            }

            return true;
        } else {
            // Unknown expression type - default to DataFusion
            return false;
        }
    }

    @Override
    public boolean canLuceneExecuteTableScan(TableScan tableScan) {
        // Lucene can always scan its own indexes
        return true;
    }

    @Override
    public boolean shouldDataFusionExecuteAggregate(Aggregate aggregate) {
        // Aggregations always go to DataFusion
        return true;
    }

    @Override
    public boolean shouldDataFusionExecuteJoin(Join join) {
        // Joins always go to DataFusion
        return true;
    }

    @Override
    public boolean shouldDataFusionExecuteSort(Sort sort) {
        // Sorts typically go to DataFusion for efficient sorting
        return true;
    }

    /**
     * Checks if an expression is a simple field reference (no computation).
     *
     * @param expression the expression to check
     * @return true if it's a simple field reference
     */
    private boolean isSimpleFieldReference(RexNode expression) {
        return expression instanceof RexInputRef;
    }
}
