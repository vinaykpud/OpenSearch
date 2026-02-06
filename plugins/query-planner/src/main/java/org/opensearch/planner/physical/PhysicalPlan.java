/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents a complete physical execution plan.
 *
 * <p>A physical plan consists of a tree of physical operators with assigned
 * execution engines. The root operator produces the final query results.
 *
 * <p><b>Key Features:</b>
 * <ul>
 *   <li>Tree structure with root operator</li>
 *   <li>JSON serialization for debugging and logging</li>
 *   <li>DOT graph export for visualization</li>
 * </ul>
 *
 * <p><b>Example Usage:</b>
 * <pre>
 * PhysicalPlan plan = new PhysicalPlan(rootOperator);
 * String json = plan.toJson();
 * String dot = plan.toDotGraph();
 * </pre>
 */
public class PhysicalPlan implements ToXContentObject {

    private final PhysicalOperator root;

    /**
     * Constructs a new PhysicalPlan.
     *
     * @param root the root operator of the plan
     */
    public PhysicalPlan(PhysicalOperator root) {
        this.root = Objects.requireNonNull(root, "root operator cannot be null");
    }

    /**
     * Gets the root operator of the plan.
     *
     * @return the root operator
     */
    public PhysicalOperator getRoot() {
        return root;
    }

    /**
     * Converts the plan to JSON format.
     *
     * @return JSON string representation
     */
    public String toJson() {
        try {
            XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.toString();
        } catch (IOException e) {
            return "{\"error\": \"Failed to serialize plan: " + e.getMessage() + "\"}";
        }
    }

    /**
     * Converts the plan to DOT graph format for visualization.
     *
     * <p>The DOT format can be rendered using Graphviz tools.
     *
     * @return DOT graph string
     */
    public String toDotGraph() {
        StringBuilder sb = new StringBuilder();
        sb.append("digraph PhysicalPlan {\n");
        sb.append("  rankdir=BT;\n"); // Bottom to top (leaf to root)
        sb.append("  node [shape=box, style=rounded];\n\n");
        
        // Generate nodes and edges
        int[] nodeId = {0};
        generateDotNodes(root, nodeId, sb);
        
        sb.append("}\n");
        return sb.toString();
    }

    private int generateDotNodes(PhysicalOperator operator, int[] nodeId, StringBuilder sb) {
        int currentId = nodeId[0]++;
        
        // Create node label
        String label = String.format(
            "%s\\n[%s]",
            operator.getOperatorType(),
            operator.getExecutionEngine()
        );
        
        // Add operator-specific details
        if (operator instanceof IndexScanOperator) {
            IndexScanOperator scan = (IndexScanOperator) operator;
            label += "\\nindex=" + scan.getIndexName();
        } else if (operator instanceof FilterOperator) {
            FilterOperator filter = (FilterOperator) operator;
            label += "\\n" + filter.getPredicate();
        } else if (operator instanceof HashAggregateOperator) {
            HashAggregateOperator agg = (HashAggregateOperator) operator;
            label += "\\ngroup=" + agg.getGroupByFields();
        } else if (operator instanceof TransferOperator) {
            TransferOperator transfer = (TransferOperator) operator;
            label += String.format("\\n%s → %s", transfer.getFromEngine(), transfer.getToEngine());
        }
        
        // Write node
        sb.append(String.format("  node%d [label=\"%s\"];\n", currentId, label));
        
        // Process children and create edges
        for (PhysicalOperator child : operator.getChildren()) {
            int childId = generateDotNodes(child, nodeId, sb);
            sb.append(String.format("  node%d -> node%d;\n", childId, currentId));
        }
        
        return currentId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("type", "PhysicalPlan");
        builder.field("root");
        operatorToXContent(root, builder, params);
        return builder;
    }

    private void operatorToXContent(PhysicalOperator operator, XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("operatorType", operator.getOperatorType().toString());
        builder.field("executionEngine", operator.getExecutionEngine().toString());
        builder.field("outputSchema", operator.getOutputSchema());
        
        // Add operator-specific fields
        if (operator instanceof IndexScanOperator) {
            IndexScanOperator scan = (IndexScanOperator) operator;
            builder.field("indexName", scan.getIndexName());
        } else if (operator instanceof FilterOperator) {
            FilterOperator filter = (FilterOperator) operator;
            builder.field("predicate", filter.getPredicate());
        } else if (operator instanceof HashAggregateOperator) {
            HashAggregateOperator agg = (HashAggregateOperator) operator;
            builder.field("groupByFields", agg.getGroupByFields());
            builder.field("aggregateFunctions", agg.getAggregateFunctions());
        } else if (operator instanceof HashJoinOperator) {
            HashJoinOperator join = (HashJoinOperator) operator;
            builder.field("joinType", join.getJoinType().toString());
            builder.field("leftKey", join.getLeftKey());
            builder.field("rightKey", join.getRightKey());
        } else if (operator instanceof SortOperator) {
            SortOperator sort = (SortOperator) operator;
            builder.startArray("sortKeys");
            for (SortOperator.SortKey key : sort.getSortKeys()) {
                builder.startObject();
                builder.field("field", key.getFieldName());
                builder.field("ascending", key.isAscending());
                builder.endObject();
            }
            builder.endArray();
        } else if (operator instanceof LimitOperator) {
            LimitOperator limit = (LimitOperator) operator;
            builder.field("limit", limit.getLimit());
            builder.field("offset", limit.getOffset());
        } else if (operator instanceof TransferOperator) {
            TransferOperator transfer = (TransferOperator) operator;
            builder.field("fromEngine", transfer.getFromEngine().toString());
            builder.field("toEngine", transfer.getToEngine().toString());
        }
        
        // Add children
        if (!operator.getChildren().isEmpty()) {
            builder.startArray("children");
            for (PhysicalOperator child : operator.getChildren()) {
                operatorToXContent(child, builder, params);
            }
            builder.endArray();
        }
        
        builder.endObject();
    }

    @Override
    public String toString() {
        return toJson();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PhysicalPlan that = (PhysicalPlan) obj;
        return Objects.equals(root, that.root);
    }

    @Override
    public int hashCode() {
        return Objects.hash(root);
    }
}
