/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalOrder;

import java.util.ArrayList;
import java.util.List;

/**
 * Mutable builder used during the aggregation tree walk to accumulate metadata.
 * Produces an immutable {@link AggregationMetadata} via {@link #build}.
 */
public class AggregationMetadataBuilder {

    /** Creates a new empty builder. */
    public AggregationMetadataBuilder() {}

    /** Name used for the implicit COUNT(*) aggregate added by bucket aggregations. */
    public static final String IMPLICIT_COUNT_NAME = "_count";

    private final List<GroupingInfo> groupings = new ArrayList<>();
    private final List<String> aggregateFieldNames = new ArrayList<>();
    private final List<AggregateCall> aggregateCalls = new ArrayList<>();
    private final List<BucketOrder> bucketOrders = new ArrayList<>();
    private boolean implicitCountRequested = false;

    /**
     * Adds a grouping contribution.
     *
     * @param grouping the grouping info to add
     */
    public void addGrouping(GroupingInfo grouping) {
        groupings.add(grouping);
    }

    /**
     * Adds a bucket order for post-aggregation sorting.
     *
     * @param order the bucket order to add (may be null or compound)
     */
    public void addBucketOrder(BucketOrder order) {
        if (order == null) return;
        if (order instanceof InternalOrder.CompoundOrder compound) {
            bucketOrders.addAll(compound.orderElements());
        } else {
            bucketOrders.add(order);
        }
    }

    /**
     * Adds a Calcite aggregate call.
     *
     * @param call the aggregate call to add
     */
    public void addAggregateCall(AggregateCall call) {
        aggregateCalls.add(call);
    }

    /**
     * Adds an output field name for an aggregate call.
     *
     * @param name the aggregate field name
     */
    public void addAggregateFieldName(String name) {
        aggregateFieldNames.add(name);
    }

    /**
     * Requests that an implicit COUNT(*) be added as {@code _count}.
     * Idempotent — nested bucket handlers can call this multiple times;
     * only one COUNT(*) is created at build time.
     */
    public void requestImplicitCount() {
        this.implicitCountRequested = true;
    }

    /**
     * Builds an immutable {@link AggregationMetadata} from the accumulated data.
     *
     * @param inputRowType The schema before aggregation (for resolving grouping field names to indices)
     * @param ctx The aggregation conversion context (for type factory access)
     * @return An immutable AggregationMetadata
     * @throws ConversionException if grouping field lookup fails
     */
    public AggregationMetadata build(RelDataType inputRowType, AggregationConversionContext ctx)
            throws ConversionException {
        List<Integer> allGroupIndices = new ArrayList<>();
        List<String> allGroupFieldNames = new ArrayList<>();
        for (GroupingInfo g : groupings) {
            allGroupIndices.addAll(g.resolveIndices(inputRowType));
            allGroupFieldNames.addAll(g.getFieldNames());
        }

        // If no GROUP BY, metric results could be null (e.g., AVG of empty set)
        // Apply nullable types to metric calls. COUNT stays non-nullable (returns 0).
        List<AggregateCall> allCalls = new ArrayList<>();
        boolean noGroupBy = groupings.isEmpty();
        for (AggregateCall call : aggregateCalls) {
            if (noGroupBy) {
                RelDataType nullableType = ctx.getTypeFactory()
                    .createTypeWithNullability(call.getType(), true);
                allCalls.add(AggregateCall.create(
                    call.getAggregation(), call.isDistinct(), call.isApproximate(),
                    call.ignoreNulls(), call.getArgList(), call.filterArg,
                    call.getCollation(), nullableType, call.getName()));
            } else {
                allCalls.add(call);
            }
        }
        List<String> allFieldNames = new ArrayList<>(aggregateFieldNames);

        if (implicitCountRequested) {
            allCalls.add(AggregateCall.create(
                SqlStdOperatorTable.COUNT,
                false,
                false,
                false,
                List.of(),
                -1,
                RelCollations.EMPTY,
                ctx.getTypeFactory().createSqlType(SqlTypeName.BIGINT),
                IMPLICIT_COUNT_NAME
            ));
            allFieldNames.add(IMPLICIT_COUNT_NAME);
        }

        return new AggregationMetadata(
            ImmutableBitSet.of(allGroupIndices),
            List.copyOf(allGroupFieldNames),
            List.copyOf(allFieldNames),
            List.copyOf(allCalls),
            List.copyOf(bucketOrders)
        );
    }
}
