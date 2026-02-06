/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Default implementation of the physical planner.
 *
 * <p>This planner converts optimized logical plans into physical execution plans
 * by performing the following steps:
 * <ol>
 *   <li>Traverse the logical plan tree (post-order)</li>
 *   <li>Convert each logical operator to a physical operator</li>
 *   <li>Assign execution engine using CapabilityDetector</li>
 *   <li>Insert Transfer operators at engine boundaries</li>
 * </ol>
 *
 * <p><b>Example Conversion:</b>
 * <pre>
 * Logical Plan:
 *   LogicalAggregate(group=[category], agg=[COUNT(*)])
 *     LogicalFilter(condition=[price > 100])
 *       LogicalTableScan(table=[products])
 *
 * Physical Plan:
 *   HashAggregateOperator[DATAFUSION]
 *     TransferOperator[LUCENE → DATAFUSION]
 *       FilterOperator[LUCENE]
 *         IndexScanOperator[LUCENE]
 * </pre>
 */
public class DefaultPhysicalPlanner implements PhysicalPlanner {

    private final CapabilityDetector capabilityDetector;

    /**
     * Constructs a new DefaultPhysicalPlanner.
     *
     * @param capabilityDetector the capability detector for engine assignment
     */
    public DefaultPhysicalPlanner(CapabilityDetector capabilityDetector) {
        this.capabilityDetector = capabilityDetector;
    }

    /**
     * Constructs a new DefaultPhysicalPlanner with default capability detector.
     */
    public DefaultPhysicalPlanner() {
        this(new DefaultCapabilityDetector());
    }

    @Override
    public PhysicalPlan generatePhysicalPlan(RelNode optimizedPlan, PlanningContext planningContext) throws PlanningException {
        if (optimizedPlan == null) {
            throw new PlanningException("Optimized plan cannot be null");
        }
        if (planningContext == null) {
            throw new PlanningException("Planning context cannot be null");
        }

        try {
            PhysicalOperator rootOperator = convertToPhysical(optimizedPlan, planningContext);
            return new PhysicalPlan(rootOperator);
        } catch (Exception e) {
            throw new PlanningException("Failed to generate physical plan: " + e.getMessage(), e);
        }
    }

    /**
     * Converts a logical operator to a physical operator.
     *
     * <p>This method recursively converts the entire operator tree,
     * processing children first (post-order traversal).
     *
     * @param logicalOp the logical operator to convert
     * @param context the planning context
     * @return the physical operator
     * @throws PlanningException if conversion fails
     */
    private PhysicalOperator convertToPhysical(RelNode logicalOp, PlanningContext context) throws PlanningException {
        // Convert children first (post-order traversal)
        List<PhysicalOperator> physicalChildren = new ArrayList<>();
        for (RelNode child : logicalOp.getInputs()) {
            physicalChildren.add(convertToPhysical(child, context));
        }

        // Convert this operator
        PhysicalOperator physicalOp = convertOperator(logicalOp, physicalChildren, context);

        // Insert Transfer operator if engine changes
        if (!physicalChildren.isEmpty()) {
            physicalOp = insertTransferIfNeeded(physicalOp, physicalChildren);
        }

        return physicalOp;
    }

    /**
     * Converts a single logical operator to a physical operator.
     *
     * @param logicalOp the logical operator
     * @param physicalChildren the converted children
     * @param context the planning context
     * @return the physical operator
     * @throws PlanningException if operator type is unsupported
     */
    private PhysicalOperator convertOperator(
        RelNode logicalOp,
        List<PhysicalOperator> physicalChildren,
        PlanningContext context
    ) throws PlanningException {
        // Detect execution engine for this operator
        ExecutionEngine engine = capabilityDetector.detectEngine(logicalOp);

        // Extract output schema
        List<String> outputSchema = extractOutputSchema(logicalOp);

        // Convert based on operator type
        if (logicalOp instanceof TableScan) {
            return convertTableScan((TableScan) logicalOp, engine, outputSchema);
        } else if (logicalOp instanceof Filter) {
            return convertFilter((Filter) logicalOp, physicalChildren, engine, outputSchema);
        } else if (logicalOp instanceof Project) {
            return convertProject((Project) logicalOp, physicalChildren, engine, outputSchema);
        } else if (logicalOp instanceof Aggregate) {
            return convertAggregate((Aggregate) logicalOp, physicalChildren, engine, outputSchema);
        } else if (logicalOp instanceof Join) {
            return convertJoin((Join) logicalOp, physicalChildren, engine, outputSchema);
        } else if (logicalOp instanceof Sort) {
            return convertSort((Sort) logicalOp, physicalChildren, engine, outputSchema);
        } else {
            throw new PlanningException("Unsupported logical operator type: " + logicalOp.getClass().getSimpleName());
        }
    }

    /**
     * Converts a LogicalTableScan to an IndexScanOperator.
     */
    private PhysicalOperator convertTableScan(TableScan tableScan, ExecutionEngine engine, List<String> outputSchema) {
        String indexName = tableScan.getTable().getQualifiedName().get(0);
        return new IndexScanOperator(indexName, outputSchema);
    }

    /**
     * Converts a LogicalFilter to a FilterOperator.
     */
    private PhysicalOperator convertFilter(
        Filter filter,
        List<PhysicalOperator> physicalChildren,
        ExecutionEngine engine,
        List<String> outputSchema
    ) {
        String predicate = filter.getCondition().toString();
        return new FilterOperator(predicate, engine, physicalChildren, outputSchema);
    }

    /**
     * Converts a LogicalProject to a ProjectOperator.
     */
    private PhysicalOperator convertProject(
        Project project,
        List<PhysicalOperator> physicalChildren,
        ExecutionEngine engine,
        List<String> outputSchema
    ) {
        // ProjectOperator doesn't need projections list - it uses outputSchema
        return new ProjectOperator(engine, physicalChildren, outputSchema);
    }

    /**
     * Converts a LogicalAggregate to a HashAggregateOperator.
     */
    private PhysicalOperator convertAggregate(
        Aggregate aggregate,
        List<PhysicalOperator> physicalChildren,
        ExecutionEngine engine,
        List<String> outputSchema
    ) {
        // Extract group by fields
        List<String> groupByFields = new ArrayList<>();
        for (int groupKey : aggregate.getGroupSet()) {
            groupByFields.add(aggregate.getInput().getRowType().getFieldNames().get(groupKey));
        }

        // Extract aggregate functions
        List<String> aggregateFunctions = aggregate.getAggCallList().stream()
            .map(AggregateCall::toString)
            .collect(Collectors.toList());

        return new HashAggregateOperator(groupByFields, aggregateFunctions, physicalChildren, outputSchema);
    }

    /**
     * Converts a LogicalJoin to a HashJoinOperator.
     */
    private PhysicalOperator convertJoin(
        Join join,
        List<PhysicalOperator> physicalChildren,
        ExecutionEngine engine,
        List<String> outputSchema
    ) {
        // Convert Calcite JoinRelType to our JoinType
        HashJoinOperator.JoinType joinType;
        switch (join.getJoinType()) {
            case INNER:
                joinType = HashJoinOperator.JoinType.INNER;
                break;
            case LEFT:
                joinType = HashJoinOperator.JoinType.LEFT;
                break;
            case RIGHT:
                joinType = HashJoinOperator.JoinType.RIGHT;
                break;
            case FULL:
                joinType = HashJoinOperator.JoinType.FULL;
                break;
            default:
                joinType = HashJoinOperator.JoinType.INNER;
        }

        // Extract join keys (simplified - assumes equi-join)
        String leftKey = "left_key";  // TODO: Extract from join condition
        String rightKey = "right_key"; // TODO: Extract from join condition

        return new HashJoinOperator(joinType, leftKey, rightKey, physicalChildren, outputSchema);
    }

    /**
     * Converts a LogicalSort to a SortOperator.
     */
    private PhysicalOperator convertSort(
        Sort sort,
        List<PhysicalOperator> physicalChildren,
        ExecutionEngine engine,
        List<String> outputSchema
    ) {
        List<SortOperator.SortKey> sortKeys = new ArrayList<>();
        
        if (sort.getCollation() != null) {
            sort.getCollation().getFieldCollations().forEach(fieldCollation -> {
                String fieldName = sort.getInput().getRowType().getFieldNames().get(fieldCollation.getFieldIndex());
                boolean ascending = fieldCollation.getDirection().isDescending() ? false : true;
                sortKeys.add(new SortOperator.SortKey(fieldName, ascending));
            });
        }

        // Handle LIMIT and OFFSET
        int limit = sort.fetch != null ? ((org.apache.calcite.rex.RexLiteral) sort.fetch).getValueAs(Integer.class) : -1;
        int offset = sort.offset != null ? ((org.apache.calcite.rex.RexLiteral) sort.offset).getValueAs(Integer.class) : 0;

        // If there's a limit, wrap in LimitOperator
        PhysicalOperator sortOp = new SortOperator(sortKeys, physicalChildren, outputSchema);
        
        if (limit > 0) {
            return new LimitOperator(limit, offset, engine, Collections.singletonList(sortOp), outputSchema);
        }

        return sortOp;
    }

    /**
     * Inserts a Transfer operator if the engine changes between parent and child.
     *
     * @param parent the parent operator
     * @param children the child operators
     * @return the parent operator, possibly with Transfer operators inserted
     */
    private PhysicalOperator insertTransferIfNeeded(PhysicalOperator parent, List<PhysicalOperator> children) {
        ExecutionEngine parentEngine = parent.getExecutionEngine();
        
        // Check if any child has a different engine
        boolean needsTransfer = children.stream()
            .anyMatch(child -> child.getExecutionEngine() != parentEngine);

        if (!needsTransfer) {
            return parent;
        }

        // Insert Transfer operators for children with different engines
        List<PhysicalOperator> newChildren = new ArrayList<>();
        for (PhysicalOperator child : children) {
            if (child.getExecutionEngine() != parentEngine) {
                // Insert Transfer operator
                TransferOperator transfer = new TransferOperator(
                    child.getExecutionEngine(),
                    parentEngine,
                    Collections.singletonList(child),
                    child.getOutputSchema()
                );
                newChildren.add(transfer);
            } else {
                newChildren.add(child);
            }
        }

        // Recreate parent with new children
        return recreateOperatorWithChildren(parent, newChildren);
    }

    /**
     * Recreates an operator with new children.
     *
     * <p>This is needed when inserting Transfer operators.
     *
     * @param operator the operator to recreate
     * @param newChildren the new children
     * @return a new operator with the same properties but different children
     */
    private PhysicalOperator recreateOperatorWithChildren(PhysicalOperator operator, List<PhysicalOperator> newChildren) {
        if (operator instanceof FilterOperator) {
            FilterOperator filter = (FilterOperator) operator;
            return new FilterOperator(
                filter.getPredicate(),
                filter.getExecutionEngine(),
                newChildren,
                filter.getOutputSchema()
            );
        } else if (operator instanceof ProjectOperator) {
            ProjectOperator project = (ProjectOperator) operator;
            return new ProjectOperator(
                project.getExecutionEngine(),
                newChildren,
                project.getOutputSchema()
            );
        } else if (operator instanceof HashAggregateOperator) {
            HashAggregateOperator agg = (HashAggregateOperator) operator;
            return new HashAggregateOperator(
                agg.getGroupByFields(),
                agg.getAggregateFunctions(),
                newChildren,
                agg.getOutputSchema()
            );
        } else if (operator instanceof HashJoinOperator) {
            HashJoinOperator join = (HashJoinOperator) operator;
            return new HashJoinOperator(
                join.getJoinType(),
                join.getLeftKey(),
                join.getRightKey(),
                newChildren,
                join.getOutputSchema()
            );
        } else if (operator instanceof SortOperator) {
            SortOperator sort = (SortOperator) operator;
            return new SortOperator(
                sort.getSortKeys(),
                newChildren,
                sort.getOutputSchema()
            );
        } else if (operator instanceof LimitOperator) {
            LimitOperator limit = (LimitOperator) operator;
            return new LimitOperator(
                limit.getLimit(),
                limit.getOffset(),
                limit.getExecutionEngine(),
                newChildren,
                limit.getOutputSchema()
            );
        } else {
            // For operators that don't need recreation (like IndexScan), return as-is
            return operator;
        }
    }

    /**
     * Extracts the output schema from a logical operator.
     *
     * @param operator the logical operator
     * @return list of field names
     */
    private List<String> extractOutputSchema(RelNode operator) {
        return operator.getRowType().getFieldList().stream()
            .map(RelDataTypeField::getName)
            .collect(Collectors.toList());
    }
}
