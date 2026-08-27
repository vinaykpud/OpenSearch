/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/** Reads Arrow vector cells as plain Java values, unwrapping Arrow {@link Text} recursively. */
public final class ArrowValues {

    // Space-separator output matches the SQL plugin's ExprTimestampValue.
    // Variable-fraction (1..9 digits, trailing zeros stripped) matches DATE_TIME_FORMATTER_VARIABLE_NANOS.
    private static final DateTimeFormatter TIMESTAMP_NO_NANO = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT);
    private static final DateTimeFormatter TIMESTAMP_WITH_NANO = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss")
        .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
        .toFormatter(Locale.ROOT);
    private static final DateTimeFormatter TIME_NO_NANO = DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT);
    private static final DateTimeFormatter TIME_WITH_NANO = new DateTimeFormatterBuilder().appendPattern("HH:mm:ss")
        .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
        .toFormatter(Locale.ROOT);
    // DataFusion CAST(temporal AS VARCHAR) — date and time joined by 'T', optional fraction.
    private static final Pattern ISO_TIMESTAMP_T = Pattern.compile("^(\\d{4}-\\d{2}-\\d{2})T(\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?)$");

    private ArrowValues() {}

    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);

    /** Converts row {@code rowId} of a {@link VectorSchemaRoot} into a JSON-friendly field map. */
    public static Map<String, Object> toSourceMap(VectorSchemaRoot root, int rowId) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Field field : root.getSchema().getFields()) {
            Object converted = toSourceValue(root.getVector(field.getName()), rowId);
            if (converted != null) {
                out.put(field.getName(), converted);
            }
        }
        return out;
    }

    /**
     * Reads an Arrow cell as a JSON-friendly scalar: numerics coerced to
     * {@code long}/{@code double}, timestamps rendered as ISO-8601 UTC strings. Binary and
     * complex (list/struct/decimal) types are not yet supported and return {@code null}.
     */
    public static Object toSourceValue(FieldVector vec, int idx) {
        if (vec == null || vec.isNull(idx)) return null;
        ArrowType type = vec.getField().getType();
        ArrowType.ArrowTypeID id = type.getTypeID();
        switch (id) {
            case Binary:
            case LargeBinary:
            case FixedSizeBinary:
            case BinaryView:
                return null;
            default:
                break;
        }
        Object raw = vec.getObject(idx);
        switch (id) {
            case Utf8:
            case LargeUtf8:
            case Utf8View, Date:
                return raw == null ? null : raw.toString();
            case Int:
                return raw instanceof Number ? ((Number) raw).longValue() : raw;
            case FloatingPoint:
                return raw instanceof Number ? ((Number) raw).doubleValue() : raw;
            case Bool:
                return raw;
            case Timestamp:
                if (raw instanceof Number) {
                    ArrowType.Timestamp ts = (ArrowType.Timestamp) type;
                    return ISO_FORMATTER.format(toInstant(((Number) raw).longValue(), ts.getUnit()));
                }
                return raw == null ? null : raw.toString();
            default:
                // TODO type coverage (list, struct, decimal)
                return null;
        }
    }

    private static Instant toInstant(long v, TimeUnit unit) {
        switch (unit) {
            case SECOND:
                return Instant.ofEpochSecond(v);
            case MILLISECOND:
                return Instant.ofEpochMilli(v);
            case MICROSECOND:
                return Instant.ofEpochSecond(v / 1_000_000L, (v % 1_000_000L) * 1_000L);
            case NANOSECOND:
            default:
                return Instant.ofEpochSecond(v / 1_000_000_000L, v % 1_000_000_000L);
        }
    }

    public static Object toJavaValue(FieldVector vector, int index) {
        if (vector.isNull(index)) return null;
        if (vector instanceof VarCharVector v) {
            return spaceSeparator(new String(v.get(index), StandardCharsets.UTF_8));
        }
        // MapVector extends ListVector — must come first.
        if (vector instanceof MapVector && vector.getObject(index) instanceof List<?> entries) {
            LinkedHashMap<String, Object> map = new LinkedHashMap<>();
            for (Object entry : entries) {
                if (!(entry instanceof Map<?, ?> e)) continue;
                Object k = e.get(MapVector.KEY_NAME);
                Object v = e.get(MapVector.VALUE_NAME);
                map.put(k instanceof Text t ? t.toString() : String.valueOf(k), normalize(v));
            }
            return map;
        }
        // A top-level `object` field is materialized as a struct (make_struct → named_struct). Read its
        // children DIRECTLY off the child vectors — NOT via getObject, which coalesces a null keyword
        // child to "" and thereby loses the absent-vs-genuinely-empty distinction. Reading the vectors
        // keeps that distinction: an absent leaf's VarChar child is null (isNull → omit, matching
        // vanilla's sparse _source), while a real empty string (e.g. status.message="") stays "" and is
        // kept (vanilla keeps it). A sub-object whose leaves are all absent collapses to {} and is also
        // omitted (R1b — vanilla omits the parent). NonNullableStructVector is the base of StructVector,
        // so this catches both. See design/nested-map-attributes/03-...-PLAN.md (R1/R1b).
        if (vector instanceof NonNullableStructVector sv) {
            LinkedHashMap<String, Object> obj = new LinkedHashMap<>();
            for (FieldVector child : sv.getChildrenFromFields()) {
                Object cv = toJavaValue(child, index);
                if (cv == null || (cv instanceof Map<?, ?> cm && cm.isEmpty())) {
                    continue;
                }
                obj.put(child.getField().getName(), cv);
            }
            // A wholly-empty object → null, not {}: vanilla renders an object with no populated leaves as
            // null (both as a nested child — where the parent's own null/empty-map check then drops it —
            // and as a top-level column cell, e.g. an empty traceGroupFields comes back null on Lucene).
            return obj.isEmpty() ? null : obj;
        }
        Object value = vector.getObject(index);
        if (vector instanceof ListVector lv && value instanceof List<?> raw) {
            Field element = lv.getDataVector().getField();
            // A nested field (LIST<STRUCT>, e.g. `events`) whose element carries a MAP child (a
            // flat_object like `events.attributes`) must render that child as a nested OBJECT to match
            // vanilla — Arrow's getObject flattens the MAP into an entry-list, and we'd otherwise emit
            // `[{"key":..,"value":..}, …]` instead of `{k:{…}}`. Field-aware rebuild handles it.
            if (element.getType() instanceof ArrowType.Struct) {
                return normalizeStructList(raw, element);
            }
            // child Arrow type drives temporal element formatting
            return normalizeList(raw, element);
        }
        Object temporal = formatTemporal(vector.getField().getType(), value);
        if (temporal != null) {
            return temporal;
        }
        return normalize(value);
    }

    /** ISO-T temporal → space separator; other strings unchanged. */
    private static String spaceSeparator(String s) {
        if (s == null) return null;
        var m = ISO_TIMESTAMP_T.matcher(s);
        return m.matches() ? m.group(1) + " " + m.group(2) : s;
    }

    private static Object formatTemporal(ArrowType type, Object value) {
        if (value == null) return null;
        if (type instanceof ArrowType.Date date) {
            return formatDate(date, value);
        }
        if (type instanceof ArrowType.Time time) {
            return formatTime(time, value);
        }
        if (type instanceof ArrowType.Timestamp ts) {
            return formatTimestamp(ts, value);
        }
        return null;
    }

    private static String formatDate(ArrowType.Date type, Object value) {
        LocalDate ld;
        if (value instanceof LocalDate d) {
            ld = d;
        } else if (value instanceof LocalDateTime ldt) {
            ld = ldt.toLocalDate();
        } else {
            long raw = ((Number) value).longValue();
            ld = switch (type.getUnit()) {
                case DAY -> LocalDate.ofEpochDay(raw);
                case MILLISECOND -> LocalDate.ofEpochDay(Math.floorDiv(raw, 86_400_000L));
            };
        }
        return ld.format(DateTimeFormatter.ISO_LOCAL_DATE);
    }

    /** Time -> HH:mm:ss[.frac]; never prefixes with the 1970 epoch date. */
    private static String formatTime(ArrowType.Time type, Object value) {
        LocalTime lt;
        if (value instanceof LocalTime t) {
            lt = t;
        } else if (value instanceof LocalDateTime ldt) {
            lt = ldt.toLocalTime();
        } else {
            long raw = ((Number) value).longValue();
            long nanoOfDay = switch (type.getUnit()) {
                case SECOND -> raw * 1_000_000_000L;
                case MILLISECOND -> raw * 1_000_000L;
                case MICROSECOND -> raw * 1_000L;
                case NANOSECOND -> raw;
            };
            lt = LocalTime.ofNanoOfDay(nanoOfDay);
        }
        return lt.getNano() == 0 ? lt.format(TIME_NO_NANO) : lt.format(TIME_WITH_NANO);
    }

    /** Timestamp -> yyyy-MM-dd HH:mm:ss[.frac]. */
    private static String formatTimestamp(ArrowType.Timestamp type, Object value) {
        LocalDateTime ldt;
        if (value instanceof LocalDateTime t) {
            ldt = t;
        } else if (value instanceof LocalDate ld) {
            ldt = ld.atStartOfDay();
        } else {
            long raw = ((Number) value).longValue();
            Instant instant = switch (type.getUnit()) {
                case SECOND -> Instant.ofEpochSecond(raw);
                case MILLISECOND -> Instant.ofEpochMilli(raw);
                case MICROSECOND -> Instant.ofEpochSecond(Math.floorDiv(raw, 1_000_000L), Math.floorMod(raw, 1_000_000L) * 1_000L);
                case NANOSECOND -> Instant.ofEpochSecond(Math.floorDiv(raw, 1_000_000_000L), Math.floorMod(raw, 1_000_000_000L));
            };
            ldt = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        }
        return ldt.getNano() == 0 ? ldt.format(TIMESTAMP_NO_NANO) : ldt.format(TIMESTAMP_WITH_NANO);
    }

    private static Object normalize(Object value) {
        if (value instanceof Text t) {
            return spaceSeparator(t.toString());
        }
        if (value instanceof String s) {
            return spaceSeparator(s);
        }
        if (value instanceof List<?> list) {
            return normalizeList(list, null);
        }
        if (value instanceof Map<?, ?> m) {
            // Faithful pass-through. The top-level `object`→struct sparse-shaping (omit absent leaves /
            // empty sub-objects, keep genuine "") is handled up front in the NonNullableStructVector
            // branch of toJavaValue, which reads child vectors and preserves the null-vs-"" distinction
            // getObject would lose — so this generic Map path must NOT prune, or it would drop a genuine
            // empty string in any residual map cell.
            LinkedHashMap<String, Object> out = new LinkedHashMap<>(m.size());
            for (Map.Entry<?, ?> entry : m.entrySet()) {
                Object k = entry.getKey();
                out.put(k instanceof Text t ? t.toString() : String.valueOf(k), normalize(entry.getValue()));
            }
            return out;
        }
        return value;
    }

    private static List<Object> normalizeList(List<?> raw, Field childField) {
        ArrowType childType = childField == null ? null : childField.getType();
        List<Object> out = new ArrayList<>(raw.size());
        for (Object element : raw) {
            Object formatted = childType == null ? null : formatTemporal(childType, element);
            out.add(formatted != null ? formatted : normalize(element));
        }
        return out;
    }

    /**
     * Field-aware normalization of a nested LIST&lt;STRUCT&gt; element list (e.g. {@code events}).
     * Each element is Arrow-materialized as a {@code Map}; we rebuild it using the element struct's
     * child fields so a MAP-typed child (a {@code flat_object} like {@code events.attributes}) becomes
     * a nested OBJECT (see {@link #mapEntriesToNestedObject}) rather than the raw
     * {@code [{"key":..,"value":..}, …]} entry-list Arrow produces. Non-map children keep their
     * existing normalization. All element fields are retained (vanilla keeps {@code attributes:{}} on
     * an event with no attributes), so no pruning here.
     */
    @SuppressWarnings("unchecked")
    private static List<Object> normalizeStructList(List<?> raw, Field structField) {
        LinkedHashMap<String, Field> childByName = new LinkedHashMap<>();
        for (Field c : structField.getChildren()) {
            childByName.put(c.getName(), c);
        }
        List<Object> out = new ArrayList<>(raw.size());
        for (Object element : raw) {
            if (!(element instanceof Map<?, ?> em)) {
                out.add(normalize(element));
                continue;
            }
            LinkedHashMap<String, Object> obj = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : em.entrySet()) {
                String name = e.getKey() instanceof Text t ? t.toString() : String.valueOf(e.getKey());
                Field cf = childByName.get(name);
                if (cf != null && cf.getType() instanceof ArrowType.Map) {
                    obj.put(name, mapEntriesToNestedObject(e.getValue()));
                } else {
                    obj.put(name, normalize(e.getValue()));
                }
            }
            out.add(obj);
        }
        return out;
    }

    /**
     * Converts an Arrow MAP value (materialized by {@code getObject} as a list of {@code {key,value}}
     * entries) into a nested object, <b>unflattening dotted keys</b> so
     * {@code feature_flag.result.reason} becomes {@code {feature_flag:{result:{reason:…}}}} — matching
     * vanilla OpenSearch's object-shaped {@code events[*].attributes}. An empty/absent map yields an
     * empty object {@code {}} (vanilla emits {@code {}} for an event with no attributes). Values stay
     * as stored (the parquet MAP is {@code MAP<Utf8,Utf8>}, so they are strings — type restoration is a
     * separate, storage-level concern; see design/nested-map-attributes R1/D2).
     */
    private static Object mapEntriesToNestedObject(Object raw) {
        LinkedHashMap<String, Object> nested = new LinkedHashMap<>();
        if (raw instanceof List<?> entries) {
            for (Object entry : entries) {
                if (!(entry instanceof Map<?, ?> e)) {
                    continue;
                }
                Object k = e.get(MapVector.KEY_NAME);
                Object v = e.get(MapVector.VALUE_NAME);
                String key = k instanceof Text t ? t.toString() : String.valueOf(k);
                insertDotted(nested, key, normalize(v));
            }
        }
        return nested;
    }

    /** Inserts {@code value} at a dotted {@code path} into {@code root}, creating intermediate objects. */
    @SuppressWarnings("unchecked")
    private static void insertDotted(Map<String, Object> root, String path, Object value) {
        int dot = path.indexOf('.');
        if (dot < 0) {
            root.put(path, value);
            return;
        }
        Map<String, Object> cur = root;
        int start = 0;
        while (dot >= 0) {
            String seg = path.substring(start, dot);
            Object next = cur.get(seg);
            if (!(next instanceof Map)) {
                next = new LinkedHashMap<String, Object>();
                cur.put(seg, next);
            }
            cur = (Map<String, Object>) next;
            start = dot + 1;
            dot = path.indexOf('.', start);
        }
        cur.put(path.substring(start), value);
    }
}
