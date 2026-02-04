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
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Lucene Collector that reads doc values for specified columns.
 *
 * <p>This collector is used during Lucene query execution to efficiently
 * read columnar doc values and convert them to Arrow vectors.
 *
 * <h2>How it works:</h2>
 * <ol>
 *   <li>For each leaf (segment), obtain DocValues accessors</li>
 *   <li>For each matching doc, read values from DocValues</li>
 *   <li>Accumulate values in memory-efficient lists</li>
 *   <li>Convert to Arrow VectorSchemaRoot when done</li>
 * </ol>
 *
 * <h2>Supported field types:</h2>
 * <ul>
 *   <li>keyword → SortedDocValues → VarCharVector</li>
 *   <li>long → NumericDocValues → BigIntVector</li>
 *   <li>integer → NumericDocValues → IntVector</li>
 *   <li>double → NumericDocValues (bits) → Float8Vector</li>
 *   <li>boolean → NumericDocValues → BitVector</li>
 * </ul>
 */
public class DocValuesCollector implements Collector {

    private final List<ColumnCollector> columnCollectors;
    private final List<String> columnNames;
    private final MapperService mapperService;
    private final int batchSize;

    private int totalDocs = 0;

    /**
     * Create a DocValuesCollector for the specified columns.
     *
     * @param columnNames Names of columns to collect
     * @param mapperService MapperService to get field types
     * @param batchSize Maximum number of docs to collect per batch
     */
    public DocValuesCollector(List<String> columnNames, MapperService mapperService, int batchSize) {
        this.columnNames = columnNames;
        this.mapperService = mapperService;
        this.batchSize = batchSize;
        this.columnCollectors = new ArrayList<>(columnNames.size());

        for (String colName : columnNames) {
            MappedFieldType fieldType = mapperService != null
                ? mapperService.fieldType(colName)
                : null;
            columnCollectors.add(createColumnCollector(colName, fieldType));
        }
    }

    /**
     * Create a ColumnCollector based on the field type.
     */
    private ColumnCollector createColumnCollector(String fieldName, MappedFieldType fieldType) {
        if (fieldType == null) {
            // Default to string if type unknown
            return new StringColumnCollector(fieldName);
        }

        String typeName = fieldType.typeName();
        switch (typeName) {
            case "keyword":
            case "text":
                return new StringColumnCollector(fieldName);
            case "long":
                return new LongColumnCollector(fieldName);
            case "integer":
            case "short":
            case "byte":
                return new IntColumnCollector(fieldName);
            case "double":
            case "float":
                return new DoubleColumnCollector(fieldName);
            case "boolean":
                return new BooleanColumnCollector(fieldName);
            default:
                return new StringColumnCollector(fieldName);
        }
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        // Initialize doc values readers for this segment
        for (ColumnCollector col : columnCollectors) {
            col.initLeaf(context);
        }

        return new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) {
                // We don't need scores
            }

            @Override
            public void collect(int doc) throws IOException {
                if (totalDocs >= batchSize) {
                    return;  // Batch full
                }

                for (ColumnCollector col : columnCollectors) {
                    col.collect(doc);
                }
                totalDocs++;
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    /**
     * Get the total number of documents collected.
     */
    public int getTotalDocs() {
        return totalDocs;
    }

    /**
     * Check if the batch is full.
     */
    public boolean isBatchFull() {
        return totalDocs >= batchSize;
    }

    /**
     * Convert collected values to Arrow VectorSchemaRoot.
     */
    public VectorSchemaRoot toVectorSchemaRoot(BufferAllocator allocator) {
        if (totalDocs == 0) {
            return null;
        }

        // Build schema
        List<Field> fields = new ArrayList<>();
        for (ColumnCollector col : columnCollectors) {
            fields.add(col.getArrowField());
        }
        Schema schema = new Schema(fields);

        // Create vectors
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

        // Set initial capacity to ensure proper buffer allocation
        for (FieldVector vector : root.getFieldVectors()) {
            vector.setInitialCapacity(totalDocs);
        }
        root.allocateNew();

        // Populate vectors
        for (int i = 0; i < columnCollectors.size(); i++) {
            ColumnCollector col = columnCollectors.get(i);
            FieldVector vector = root.getVector(i);
            col.fillVector(vector);
        }

        root.setRowCount(totalDocs);
        return root;
    }

    /**
     * Clear collected data for reuse.
     */
    public void reset() {
        for (ColumnCollector col : columnCollectors) {
            col.reset();
        }
        totalDocs = 0;
    }

    // ========== Column Collector Implementations ==========

    /**
     * Base class for column collectors.
     */
    private abstract static class ColumnCollector {
        protected final String fieldName;

        ColumnCollector(String fieldName) {
            this.fieldName = fieldName;
        }

        abstract void initLeaf(LeafReaderContext context) throws IOException;
        abstract void collect(int doc) throws IOException;
        abstract Field getArrowField();
        abstract void fillVector(FieldVector vector);
        abstract void reset();
    }

    /**
     * Collector for string/keyword fields using SortedSetDocValues.
     * OpenSearch uses SORTED_SET for keyword fields to support multi-valued.
     */
    private static class StringColumnCollector extends ColumnCollector {
        private SortedSetDocValues docValues;
        private final List<String> values = new ArrayList<>();

        StringColumnCollector(String fieldName) {
            super(fieldName);
        }

        @Override
        void initLeaf(LeafReaderContext context) throws IOException {
            try {
                docValues = DocValues.getSortedSet(context.reader(), fieldName);
            } catch (IllegalStateException e) {
                // Field doesn't have doc values (e.g., _id)
                docValues = null;
            }
        }

        @Override
        void collect(int doc) throws IOException {
            if (docValues != null && docValues.advanceExact(doc) && docValues.docValueCount() > 0) {
                // Take first value for multi-valued field
                long ord = docValues.nextOrd();
                values.add(docValues.lookupOrd(ord).utf8ToString());
            } else {
                values.add(null);
            }
        }

        @Override
        Field getArrowField() {
            return new Field(fieldName, FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        }

        @Override
        void fillVector(FieldVector vector) {
            VarCharVector v = (VarCharVector) vector;
            for (int i = 0; i < values.size(); i++) {
                String val = values.get(i);
                if (val != null) {
                    v.setSafe(i, val.getBytes());
                } else {
                    // Explicitly set null to ensure validity buffer is properly allocated
                    v.setNull(i);
                }
            }
        }

        @Override
        void reset() {
            values.clear();
        }
    }

    /**
     * Collector for long fields.
     * OpenSearch stores numeric fields as SORTED_NUMERIC doc values.
     */
    private static class LongColumnCollector extends ColumnCollector {
        private SortedNumericDocValues docValues;
        private final List<Long> values = new ArrayList<>();
        private final List<Boolean> nulls = new ArrayList<>();

        LongColumnCollector(String fieldName) {
            super(fieldName);
        }

        @Override
        void initLeaf(LeafReaderContext context) throws IOException {
            try {
                docValues = DocValues.getSortedNumeric(context.reader(), fieldName);
            } catch (IllegalStateException e) {
                // Field doesn't have doc values
                docValues = null;
            }
        }

        @Override
        void collect(int doc) throws IOException {
            if (docValues != null && docValues.advanceExact(doc) && docValues.docValueCount() > 0) {
                values.add(docValues.nextValue());
                nulls.add(false);
            } else {
                values.add(0L);
                nulls.add(true);
            }
        }

        @Override
        Field getArrowField() {
            return new Field(fieldName, FieldType.nullable(new ArrowType.Int(64, true)), null);
        }

        @Override
        void fillVector(FieldVector vector) {
            BigIntVector v = (BigIntVector) vector;
            for (int i = 0; i < values.size(); i++) {
                if (!nulls.get(i)) {
                    v.setSafe(i, values.get(i));
                } else {
                    v.setNull(i);
                }
            }
        }

        @Override
        void reset() {
            values.clear();
            nulls.clear();
        }
    }

    /**
     * Collector for integer fields.
     * OpenSearch stores numeric fields as SORTED_NUMERIC doc values.
     */
    private static class IntColumnCollector extends ColumnCollector {
        private SortedNumericDocValues docValues;
        private final List<Integer> values = new ArrayList<>();
        private final List<Boolean> nulls = new ArrayList<>();

        IntColumnCollector(String fieldName) {
            super(fieldName);
        }

        @Override
        void initLeaf(LeafReaderContext context) throws IOException {
            try {
                docValues = DocValues.getSortedNumeric(context.reader(), fieldName);
            } catch (IllegalStateException e) {
                // Field doesn't have doc values
                docValues = null;
            }
        }

        @Override
        void collect(int doc) throws IOException {
            if (docValues != null && docValues.advanceExact(doc) && docValues.docValueCount() > 0) {
                values.add((int) docValues.nextValue());
                nulls.add(false);
            } else {
                values.add(0);
                nulls.add(true);
            }
        }

        @Override
        Field getArrowField() {
            return new Field(fieldName, FieldType.nullable(new ArrowType.Int(32, true)), null);
        }

        @Override
        void fillVector(FieldVector vector) {
            IntVector v = (IntVector) vector;
            for (int i = 0; i < values.size(); i++) {
                if (!nulls.get(i)) {
                    v.setSafe(i, values.get(i));
                } else {
                    v.setNull(i);
                }
            }
        }

        @Override
        void reset() {
            values.clear();
            nulls.clear();
        }
    }

    /**
     * Collector for double fields.
     * OpenSearch stores numeric fields as SORTED_NUMERIC doc values.
     */
    private static class DoubleColumnCollector extends ColumnCollector {
        private SortedNumericDocValues docValues;
        private final List<Double> values = new ArrayList<>();
        private final List<Boolean> nulls = new ArrayList<>();

        DoubleColumnCollector(String fieldName) {
            super(fieldName);
        }

        @Override
        void initLeaf(LeafReaderContext context) throws IOException {
            try {
                docValues = DocValues.getSortedNumeric(context.reader(), fieldName);
            } catch (IllegalStateException e) {
                // Field doesn't have doc values
                docValues = null;
            }
        }

        @Override
        void collect(int doc) throws IOException {
            if (docValues != null && docValues.advanceExact(doc) && docValues.docValueCount() > 0) {
                // Doubles are stored as long bits in doc values
                values.add(Double.longBitsToDouble(docValues.nextValue()));
                nulls.add(false);
            } else {
                values.add(0.0);
                nulls.add(true);
            }
        }

        @Override
        Field getArrowField() {
            return new Field(fieldName, FieldType.nullable(new ArrowType.FloatingPoint(
                org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)), null);
        }

        @Override
        void fillVector(FieldVector vector) {
            Float8Vector v = (Float8Vector) vector;
            for (int i = 0; i < values.size(); i++) {
                if (!nulls.get(i)) {
                    v.setSafe(i, values.get(i));
                } else {
                    v.setNull(i);
                }
            }
        }

        @Override
        void reset() {
            values.clear();
            nulls.clear();
        }
    }

    /**
     * Collector for boolean fields.
     * OpenSearch stores boolean fields as SORTED_NUMERIC doc values.
     */
    private static class BooleanColumnCollector extends ColumnCollector {
        private SortedNumericDocValues docValues;
        private final List<Boolean> values = new ArrayList<>();
        private final List<Boolean> nulls = new ArrayList<>();

        BooleanColumnCollector(String fieldName) {
            super(fieldName);
        }

        @Override
        void initLeaf(LeafReaderContext context) throws IOException {
            try {
                docValues = DocValues.getSortedNumeric(context.reader(), fieldName);
            } catch (IllegalStateException e) {
                // Field doesn't have doc values
                docValues = null;
            }
        }

        @Override
        void collect(int doc) throws IOException {
            if (docValues != null && docValues.advanceExact(doc) && docValues.docValueCount() > 0) {
                values.add(docValues.nextValue() != 0);
                nulls.add(false);
            } else {
                values.add(false);
                nulls.add(true);
            }
        }

        @Override
        Field getArrowField() {
            return new Field(fieldName, FieldType.nullable(ArrowType.Bool.INSTANCE), null);
        }

        @Override
        void fillVector(FieldVector vector) {
            BitVector v = (BitVector) vector;
            for (int i = 0; i < values.size(); i++) {
                if (!nulls.get(i)) {
                    v.setSafe(i, values.get(i) ? 1 : 0);
                } else {
                    v.setNull(i);
                }
            }
        }

        @Override
        void reset() {
            values.clear();
            nulls.clear();
        }
    }
}
