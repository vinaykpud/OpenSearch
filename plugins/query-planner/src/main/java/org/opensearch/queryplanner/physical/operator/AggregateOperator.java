/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.physical.operator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.queryplanner.physical.exec.ExecAggregate.AggMode;
import org.opensearch.queryplanner.physical.operator.Operator;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Aggregate operator - groups and aggregates rows.
 */
public class AggregateOperator implements Operator {

    // Allow $ in column names and aliases (Calcite uses EXPR$N for generated names)
    private static final Pattern AGG_PATTERN = Pattern.compile("(\\w+)\\(([^)]+)\\)(?:\\s+AS\\s+([\\w$]+))?", Pattern.CASE_INSENSITIVE);

    private final Operator child;
    private final List<String> groupBy;
    private final List<AggFunc> aggregates;
    private final AggMode mode;

    private BufferAllocator allocator;
    private Map<GroupKey, Accumulator[]> groups;
    private Schema inputSchema;
    private boolean consumed = false;

    public AggregateOperator(Operator child, List<String> groupBy, List<String> aggregateExprs, AggMode mode) {
        this.child = child;
        this.groupBy = groupBy;
        this.aggregates = parseAggregates(aggregateExprs);
        this.mode = mode;
    }

    private List<AggFunc> parseAggregates(List<String> exprs) {
        List<AggFunc> result = new ArrayList<>();
        for (String expr : exprs) {
            Matcher m = AGG_PATTERN.matcher(expr.trim());
            if (m.matches()) {
                String func = m.group(1).toUpperCase();
                String column = m.group(2).trim();
                String alias = m.group(3) != null ? m.group(3) : func.toLowerCase() + "_" + column;
                result.add(new AggFunc(func, column, alias));
            } else {
                throw new IllegalArgumentException("Invalid aggregate expression: " + expr);
            }
        }
        return result;
    }

    @Override
    public void open(BufferAllocator allocator) {
        this.allocator = allocator;
        this.groups = new HashMap<>();
        this.consumed = false;
        child.open(allocator);
    }

    @Override
    public VectorSchemaRoot next() {
        if (consumed) {
            return null;
        }

        // Consume all input
        VectorSchemaRoot input;
        while ((input = child.next()) != null) {
            if (inputSchema == null) {
                inputSchema = input.getSchema();
            }
            accumulate(input);
            input.close();
        }

        consumed = true;
        return buildResult();
    }

    private void accumulate(VectorSchemaRoot batch) {
        for (int row = 0; row < batch.getRowCount(); row++) {
            GroupKey key = extractKey(batch, row);
            Accumulator[] accs = groups.computeIfAbsent(key, k -> createAccumulators());

            for (int i = 0; i < aggregates.size(); i++) {
                AggFunc agg = aggregates.get(i);
                Object value;
                if (mode == AggMode.FINAL) {
                    // In FINAL mode, read aggregate values by position (after group columns)
                    // This is more robust than looking up by name since different engines
                    // may use different naming conventions (e.g., DataFusion uses "sum(col)")
                    int colIndex = groupBy.size() + i;
                    FieldVector vector = batch.getVector(colIndex);
                    value = vector != null && !vector.isNull(row) ? getValueByVector(vector, row) : null;
                } else if (agg.func.equals("COUNT") && agg.column.equals("*")) {
                    // COUNT(*) in PARTIAL/FULL mode counts all rows - use a non-null sentinel
                    value = 1L;
                } else {
                    value = getValue(batch, agg.column, row);
                }
                accs[i].add(value);
            }
        }
    }

    private Object getValueByVector(FieldVector vector, int row) {
        if (vector instanceof BigIntVector) {
            return ((BigIntVector) vector).get(row);
        } else if (vector instanceof Float8Vector) {
            return ((Float8Vector) vector).get(row);
        } else if (vector instanceof VarCharVector) {
            return new String(((VarCharVector) vector).get(row), StandardCharsets.UTF_8);
        }
        return vector.getObject(row);
    }

    private GroupKey extractKey(VectorSchemaRoot batch, int row) {
        Object[] values = new Object[groupBy.size()];
        for (int i = 0; i < groupBy.size(); i++) {
            values[i] = getValue(batch, groupBy.get(i), row);
        }
        return new GroupKey(values);
    }

    private Object getValue(VectorSchemaRoot batch, String column, int row) {
        FieldVector vector = batch.getVector(column);
        if (vector == null || vector.isNull(row)) {
            return null;
        }

        if (vector instanceof BigIntVector) {
            return ((BigIntVector) vector).get(row);
        } else if (vector instanceof Float8Vector) {
            return ((Float8Vector) vector).get(row);
        } else if (vector instanceof VarCharVector) {
            return new String(((VarCharVector) vector).get(row), StandardCharsets.UTF_8);
        }
        return vector.getObject(row);
    }

    private Accumulator[] createAccumulators() {
        Accumulator[] result = new Accumulator[aggregates.size()];
        for (int i = 0; i < aggregates.size(); i++) {
            result[i] = createAccumulator(aggregates.get(i).func);
        }
        return result;
    }

    private Accumulator createAccumulator(String func) {
        // In FINAL mode, we're merging partial results:
        // - COUNT partials are summed (partial counts become a sum)
        // - SUM, MIN, MAX work the same way (sum sums, min of mins, max of maxes)
        switch (func) {
            case "SUM": return new SumAccumulator();
            case "COUNT":
                // FINAL mode: sum the partial counts
                // PARTIAL/FULL mode: count rows
                return mode == AggMode.FINAL ? new SumToLongAccumulator() : new CountAccumulator();
            case "MIN": return new MinAccumulator();
            case "MAX": return new MaxAccumulator();
            default: throw new UnsupportedOperationException("Unknown aggregate: " + func);
        }
    }

    private VectorSchemaRoot buildResult() {
        Schema outputSchema = buildOutputSchema();
        VectorSchemaRoot output = VectorSchemaRoot.create(outputSchema, allocator);

        int row = 0;
        for (Map.Entry<GroupKey, Accumulator[]> entry : groups.entrySet()) {
            GroupKey key = entry.getKey();
            Accumulator[] accs = entry.getValue();

            // Write group key columns
            for (int i = 0; i < groupBy.size(); i++) {
                setVectorValue(output.getVector(i), row, key.values[i]);
            }

            // Write aggregate columns
            for (int i = 0; i < aggregates.size(); i++) {
                int colIndex = groupBy.size() + i;
                setVectorValue(output.getVector(colIndex), row, accs[i].result());
            }

            row++;
        }

        // Handle empty result (no groups means one row with aggregates)
        if (groups.isEmpty() && groupBy.isEmpty()) {
            Accumulator[] accs = createAccumulators();
            for (int i = 0; i < aggregates.size(); i++) {
                setVectorValue(output.getVector(i), 0, accs[i].result());
            }
            row = 1;
        }

        output.setRowCount(row);
        return output;
    }

    private Schema buildOutputSchema() {
        List<Field> fields = new ArrayList<>();

        // Group by columns
        for (String col : groupBy) {
            Field inputField = inputSchema != null ? inputSchema.findField(col) : null;
            if (inputField != null) {
                fields.add(inputField);
            } else {
                fields.add(new Field(col, FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
            }
        }

        // Aggregate columns
        for (AggFunc agg : aggregates) {
            ArrowType type = agg.func.equals("COUNT")
                ? new ArrowType.Int(64, true)
                : new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            fields.add(new Field(agg.alias, FieldType.nullable(type), null));
        }

        return new Schema(fields);
    }

    private void setVectorValue(FieldVector vector, int row, Object value) {
        if (value == null) {
            return;
        }

        if (vector instanceof BigIntVector) {
            ((BigIntVector) vector).setSafe(row, ((Number) value).longValue());
        } else if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).setSafe(row, ((Number) value).doubleValue());
        } else if (vector instanceof VarCharVector) {
            ((VarCharVector) vector).setSafe(row, value.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void close() {
        child.close();
        groups.clear();
    }

    // Helper classes

    private static class AggFunc {
        final String func;
        final String column;
        final String alias;

        AggFunc(String func, String column, String alias) {
            this.func = func;
            this.column = column;
            this.alias = alias;
        }
    }

    private static class GroupKey {
        final Object[] values;

        GroupKey(Object[] values) {
            this.values = values;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof GroupKey)) return false;
            GroupKey other = (GroupKey) o;
            if (values.length != other.values.length) return false;
            for (int i = 0; i < values.length; i++) {
                if (values[i] == null && other.values[i] == null) continue;
                if (values[i] == null || !values[i].equals(other.values[i])) return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int h = 1;
            for (Object v : values) {
                h = 31 * h + (v == null ? 0 : v.hashCode());
            }
            return h;
        }
    }

    interface Accumulator {
        void add(Object value);
        Object result();
    }

    private static class SumAccumulator implements Accumulator {
        private double sum = 0;
        public void add(Object v) { if (v != null) sum += ((Number) v).doubleValue(); }
        public Object result() { return sum; }
    }

    private static class CountAccumulator implements Accumulator {
        private long count = 0;
        public void add(Object v) { if (v != null) count++; }
        public Object result() { return count; }
    }

    /**
     * Accumulator that sums values but returns a long.
     * Used for FINAL COUNT to sum partial counts.
     */
    private static class SumToLongAccumulator implements Accumulator {
        private long sum = 0;
        public void add(Object v) { if (v != null) sum += ((Number) v).longValue(); }
        public Object result() { return sum; }
    }

    private static class MinAccumulator implements Accumulator {
        private Double min = null;
        public void add(Object v) {
            if (v != null) {
                double d = ((Number) v).doubleValue();
                if (min == null || d < min) min = d;
            }
        }
        public Object result() { return min != null ? min : 0.0; }
    }

    private static class MaxAccumulator implements Accumulator {
        private Double max = null;
        public void add(Object v) {
            if (v != null) {
                double d = ((Number) v).doubleValue();
                if (max == null || d > max) max = d;
            }
        }
        public Object result() { return max != null ? max : 0.0; }
    }
}
