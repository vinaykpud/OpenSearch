/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical.operators;

import java.util.List;
import java.util.Objects;

/**
 * Physical operator for hash-based joins.
 *
 * <p>This operator performs joins using hash tables. It is best suited
 * for equi-joins on large datasets.
 *
 * <p><b>Execution:</b> Always executed by the DataFusion engine.
 *
 * <p><b>Join Types:</b>
 * <ul>
 *   <li>INNER: Returns rows that have matching values in both tables</li>
 *   <li>LEFT: Returns all rows from left table, matching rows from right</li>
 *   <li>RIGHT: Returns all rows from right table, matching rows from left</li>
 *   <li>FULL: Returns all rows when there is a match in either table</li>
 * </ul>
 */
public class HashJoinOperator extends PhysicalOperator {

    /**
     * Enum representing join types.
     */
    public enum JoinType {
        INNER,
        LEFT,
        RIGHT,
        FULL
    }

    private final JoinType joinType;
    private final String leftKey;
    private final String rightKey;

    /**
     * Constructs a new HashJoinOperator.
     *
     * @param joinType the type of join
     * @param leftKey the join key from the left input
     * @param rightKey the join key from the right input
     * @param children the input operators (must be exactly 2: left and right)
     * @param outputSchema the output schema (combined fields from both inputs)
     */
    public HashJoinOperator(
        JoinType joinType,
        String leftKey,
        String rightKey,
        List<PhysicalOperator> children,
        List<String> outputSchema
    ) {
        super(OperatorType.HASH_JOIN, ExecutionEngine.DATAFUSION, children, outputSchema);
        if (children == null || children.size() != 2) {
            throw new IllegalArgumentException("HashJoinOperator requires exactly 2 children");
        }
        this.joinType = Objects.requireNonNull(joinType, "joinType cannot be null");
        this.leftKey = Objects.requireNonNull(leftKey, "leftKey cannot be null");
        this.rightKey = Objects.requireNonNull(rightKey, "rightKey cannot be null");
    }

    /**
     * Gets the join type.
     *
     * @return the join type
     */
    public JoinType getJoinType() {
        return joinType;
    }

    /**
     * Gets the left join key.
     *
     * @return the left key
     */
    public String getLeftKey() {
        return leftKey;
    }

    /**
     * Gets the right join key.
     *
     * @return the right key
     */
    public String getRightKey() {
        return rightKey;
    }

    @Override
    public <T> T accept(PhysicalOperatorVisitor<T> visitor) {
        return visitor.visitHashJoin(this);
    }

    @Override
    public String toString() {
        return String.format(
            "HashJoinOperator[type=%s, leftKey=%s, rightKey=%s, schema=%s]",
            joinType,
            leftKey,
            rightKey,
            getOutputSchema()
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        HashJoinOperator that = (HashJoinOperator) obj;
        return joinType == that.joinType
            && Objects.equals(leftKey, that.leftKey)
            && Objects.equals(rightKey, that.rightKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), joinType, leftKey, rightKey);
    }
}
