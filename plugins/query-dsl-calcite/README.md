# OpenSearch DSL to Calcite Converter Plugin

An OpenSearch plugin that converts OpenSearch Query DSL to Apache Calcite logical plans (RelNode). This plugin represents OpenSearch queries as relational algebra for query optimization and execution by alternative query engines.

## Overview

This plugin integrates with OpenSearch's search pipeline to convert DSL queries into Calcite's logical plan representation. It supports:

- **Query Types**: Term, Range, Bool (must + filter), Match All
- **Aggregations**: Metric (avg, sum, min, max, count) and bucket (terms, multi\_terms)
- **Sorting**: Pre-aggregation and post-aggregation sorting with BucketOrder
- **Pagination**: Offset and fetch (limit)
- **Projection**: Source filtering (\_source includes, wildcards)
- **Dynamic Schema**: Automatic schema discovery from index mappings

## Architecture

The plugin uses the **PluginsService pattern** for integration with OpenSearch core:

```
TransportSearchAction
    ↓
PluginsService.filterPlugins(DslConverterPlugin.class)
    ↓
DslCalcitePlugin.convertDsl()
    ↓
CalciteConverterService.getConverter()
    ↓
CalciteConverterImpl.convert()
    ↓
RelNode (Calcite Logical Plan)
```

### Conversion Pipeline

`CalciteConverterImpl.convert()` builds the plan in seven steps:

1. **TableScan** — discover index schema from mappings, create `LogicalTableScan`
2. **Filter** — convert DSL `QueryBuilder` → `LogicalFilter` with `RexNode` conditions
3. **Sort** — convert top-level `SortBuilder` list → `LogicalSort` with collations
4. **Aggregate** — convert bucket/metric aggregations → `LogicalAggregate`
5. **Post-aggregation sort** — apply BucketOrder collations after aggregation
6. **Project** — apply `_source` filtering → `LogicalProject`
7. **Pagination** — apply `from`/`size` → `LogicalSort` with offset/fetch

### Key Components

| Class | Role |
|-------|------|
| `DslCalcitePlugin` | Main plugin class implementing `DslConverterPlugin` |
| `CalciteConverterService` | Manages Calcite schema and converter instances |
| `CalciteConverterImpl` | Core converter: DSL → RelNode pipeline |
| `RexNodeQueryVisitor` | Visitor converting QueryBuilders to RexNode filter expressions |
| `AggregateCallVisitor` | Visitor converting metric aggregation builders to AggregateCall |
| `AggregationInfo` | Extracts GROUP BY fields, metric fields, and post-agg sort info |
| `IndexMappingClient` | Retrieves and flattens OpenSearch index mappings |
| `OpenSearchTypeMapper` | Maps OpenSearch field types to Calcite SQL types |
| `OpenSearchFunctions` | Defines custom Calcite functions for OpenSearch operations |

## Supported Features

### Queries

| DSL Query | Calcite Representation                                                    |
|-----------|---------------------------------------------------------------------------|
| `term` | `=($field, value)` — equality filter                                      |
| `range` (gte, lte, gt, lt) | `AND(>=($field, min), <=($field, max))` — range filter                    |
| `bool` (must + filter) | `AND(condition1, condition2, ...)` — flattened conjunction                |
| `match_all` | Skipped (boolean literal `TRUE`)                                          |
| `exists` | `IS NOT NULL($field)` — field existence check & boost not supported check |

### Aggregations

| DSL Aggregation | Calcite Representation |
|-----------------|------------------------|
| `terms` | `GROUP BY $field` in `LogicalAggregate` |
| `multi_terms` | `GROUP BY $field1, $field2, ...` in `LogicalAggregate` |
| `avg` | `AVG($field)` metric in `LogicalAggregate` |
| `sum` | `SUM($field)` metric in `LogicalAggregate` |
| `min` | `MIN($field)` metric in `LogicalAggregate` |
| `max` | `MAX($field)` metric in `LogicalAggregate` |
| implicit `count` | `COUNT()` — always appended as `_count` |

### Post-Aggregation Sorting

BucketOrder on `terms`/`multi_terms` aggregations produces a `LogicalSort` wrapping the `LogicalAggregate`:

- `_count` asc/desc → sort on the `_count` column
- `_key` asc/desc → sort on GROUP BY column(s)
- Sub-aggregation name (e.g. `avg_price`) → sort on that metric column

When no explicit order is specified, OpenSearch defaults to `[_count DESC, _key ASC]`.

### Sort, Pagination, Projection

| Feature | Calcite Representation |
|---------|------------------------|
| Field sort (ASC/DESC) | `LogicalSort` with `RelFieldCollation` |
| `from` / `size` | `LogicalSort` with `offset` / `fetch` |
| `_source` includes | `LogicalProject` selecting named fields |
| `_source: false` | Empty projection |
| Wildcard patterns in `_source` | Regex-expanded `LogicalProject` |

## Examples

All examples below are drawn from the integration tests. Field references use Calcite's positional `$N` notation where the index is determined by alphabetical field ordering from the index mapping.

### 1. Term Query

```json
{
  "query": {
    "term": { "category": "electronics" }
  }
}
```
**Mapping:** `category: keyword, price: long`
```
LogicalFilter(condition=[=($0, 'electronics')])
  LogicalTableScan(table=[[test-term-query]])
```

### 2. Range Query

```json
{
  "query": {
    "range": {
      "price": { "gte": 100, "lte": 500 }
    }
  }
}
```
**Mapping:** `price: long`
```
LogicalFilter(condition=[AND(>=($0, 100), <=($0, 500))])
  LogicalTableScan(table=[[test-range-query]])
```

### 3. Bool Query (must + filter)

```json
{
  "query": {
    "bool": {
      "must": [
        { "term": { "category": "electronics" } }
      ],
      "filter": [
        { "range": { "price": { "gte": 100, "lte": 500 } } }
      ]
    }
  }
}
```
**Mapping:** `category: keyword, price: long`
```
LogicalFilter(condition=[AND(=($0, 'electronics'), >=($1, 100), <=($1, 500))])
  LogicalTableScan(table=[[test-bool-query]])
```

Both `must` and `filter` clauses are flattened into a single AND conjunction.

### 4. Sort

```json
{
  "sort": [
    { "price": { "order": "asc" } }
  ]
}
```
**Mapping:** `price: long, name: keyword`
```
LogicalSort(sort0=[$0], dir0=[ASC])
  LogicalTableScan(table=[[test-sort]])
```

### 5. Pagination

```json
{
  "from": 10,
  "size": 20
}
```
**Mapping:** `title: text`
```
LogicalSort(offset=[10], fetch=[20])
  LogicalTableScan(table=[[test-pagination]])
```

### 6. Source Filtering (Projection)

```json
{
  "_source": ["title", "price"]
}
```
**Mapping:** `title: text, price: long, brand: keyword, description: text`
```
LogicalProject(title=[$0], price=[$1])
  LogicalTableScan(table=[[test-source-filtering]])
```

Only the requested fields appear in the projection. `brand` and `description` are excluded.

### 7. Metric Aggregation (avg)

```json
{
  "aggs": {
    "avg_price": {
      "avg": { "field": "price" }
    }
  },
  "size": 0
}
```
**Mapping:** `price: long`
```
LogicalAggregate(group=[{}], avg_price=[AVG($0)], _count=[COUNT()])
  LogicalTableScan(table=[[test-metric-agg]])
```

`group=[{}]` means no GROUP BY (global aggregation). An implicit `_count` is always appended.

### 8. Terms Aggregation with Sub-Aggregation

```json
{
  "aggs": {
    "by_brand": {
      "terms": { "field": "brand" },
      "aggs": {
        "avg_price": { "avg": { "field": "price" } }
      }
    }
  },
  "size": 0
}
```
**Mapping:** `brand: keyword, price: long`
```
LogicalSort(sort0=[$2], sort1=[$0], dir0=[DESC], dir1=[ASC])
  LogicalAggregate(group=[{0}], avg_price=[AVG($1)], _count=[COUNT()])
    LogicalTableScan(table=[[test-terms-agg]])
```

No explicit order → defaults to `[_count DESC, _key ASC]`. Post-agg schema: `[brand(0), avg_price(1), _count(2)]`, so `sort0=[$2]` is `_count DESC` and `sort1=[$0]` is `_key ASC`.

### 9. Post-Aggregation Sorting (explicit order)

```json
{
  "aggs": {
    "by_brand": {
      "terms": {
        "field": "brand",
        "order": { "avg_price": "desc" }
      },
      "aggs": {
        "avg_price": { "avg": { "field": "price" } }
      }
    }
  },
  "size": 0
}
```
**Mapping:** `brand: keyword, price: long`
```
LogicalSort(sort0=[$1], sort1=[$0], dir0=[DESC], dir1=[ASC])
  LogicalAggregate(group=[{0}], avg_price=[AVG($1)], _count=[COUNT()])
    LogicalTableScan(table=[[test-post-agg-sort]])
```

Explicit `"order": {"avg_price": "desc"}` sorts by the `avg_price` metric. OpenSearch appends a `_key ASC` tie-breaker automatically.

### 10. Multi-Terms Aggregation

```json
{
  "size": 0,
  "aggs": {
    "hot": {
      "multi_terms": {
        "terms": [{"field": "region"}, {"field": "host"}],
        "order": [{"max-cpu": "desc"}, {"max-memory": "desc"}]
      },
      "aggs": {
        "max-cpu": { "max": { "field": "cpu" } },
        "max-memory": { "max": { "field": "memory" } }
      }
    }
  }
}
```
**Mapping:** `region: keyword, host: keyword, cpu: long, memory: long`
```
LogicalSort(sort0=[$2], sort1=[$3], sort2=[$0], dir0=[DESC], dir1=[DESC], dir2=[ASC])
  LogicalAggregate(group=[{1, 3}], max-cpu=[MAX($0)], max-memory=[MAX($2)], _count=[COUNT()])
    LogicalTableScan(table=[[test-multi-terms-agg]])
```

Fields are in alphabetical order: `cpu(0), host(1), memory(2), region(3)`. GROUP BY on `host(1)` and `region(3)`.

### 11. Complex Query (all features combined)

```json
{
  "query": {
    "bool": {
      "must": [
        { "term": { "brand": "dell" } }
      ],
      "filter": [
        { "term": { "category": "electronics" } },
        { "range": { "price": { "gte": 500, "lte": 2000 } } }
      ]
    }
  },
  "aggs": {
    "by_brand": {
      "terms": { "field": "brand" },
      "aggs": {
        "avg_price": { "avg": { "field": "price" } }
      }
    }
  },
  "sort": [
    { "price": { "order": "asc" } }
  ],
  "from": 0,
  "size": 10
}
```
**Mapping:** `brand: keyword, category: keyword, price: long`
```
LogicalAggregate(group=[{0}], avg_price=[AVG($2)], _count=[COUNT()])
  LogicalSort(sort0=[$2], dir0=[ASC])
    LogicalFilter(condition=[AND(=($0, 'dell'), =($1, 'electronics'), >=($2, 500), <=($2, 2000))])
      LogicalTableScan(table=[[test-complex-query]])
```

Note: Pagination (`from`/`size`) is not applied when aggregations are present.

## Type Mappings

OpenSearch field types are mapped to Calcite SQL types:

| OpenSearch Type | Calcite Type |
|-----------------|--------------|
| keyword, text, match\_only\_text | VARCHAR |
| long | BIGINT |
| integer, token\_count | INTEGER |
| short | SMALLINT |
| byte | TINYINT |
| double, scaled\_float | DOUBLE |
| float, half\_float | FLOAT |
| boolean | BOOLEAN |
| date, date\_nanos | TIMESTAMP |
| binary | VARBINARY |
| ip, completion | VARCHAR |
| object, nested, geo\_point, geo\_shape, alias | ANY |

Unknown types default to `ANY`. Type names are normalized (underscores removed, lowercased) before matching.

## Building

```bash
./gradlew :plugins:query-dsl-calcite:assemble
```

The plugin ZIP is created at `plugins/query-dsl-calcite/build/distributions/query-dsl-calcite-<version>.zip`.

## Installation

### Development Mode

```bash
./gradlew run -PinstalledPlugins="['query-dsl-calcite']"
```

### Production Installation

```bash
bin/opensearch-plugin install file:///path/to/query-dsl-calcite-<version>.zip
```

## Testing

Run integration tests:

```bash
./gradlew :plugins:query-dsl-calcite:internalClusterTest
```

Run a specific test:

```bash
./gradlew :plugins:query-dsl-calcite:internalClusterTest --tests "DslCalciteIntegrationIT.testComplexQueryConversion"
```

## Limitations

Current limitations:

1. **Read-only** — converts queries to logical plans but does not execute them
2. **Logging only** — converted plans are logged, not used for query execution
3. **Limited query types** — only `term`, `range`, `bool` (must + filter), and `match_all`
4. **Bool query** — `should` and `must_not` clauses are not yet supported
5. **Nested objects** — flattened using dot notation, no true nested query support
6. **Pagination with aggregations** — `from`/`size` is not applied when aggregations are present
7. **Sort** — only `FieldSortBuilder` (no script sort, geo sort, or nested sort)

## Dependencies

- **Apache Calcite Core**: 1.38.0
- **Apache Calcite Linq4j**: 1.38.0
- **Guava**: 33.4.0-jre
- **Commons Lang3**: 3.17.0

## Contributing

This plugin follows OpenSearch development guidelines:

1. Run `./gradlew spotlessApply` to format code
2. Run `./gradlew precommit` for validation
3. Add tests for new features
4. Sign commits with DCO: `git commit -s`

## License

This plugin is licensed under the Apache License, Version 2.0.

## References

- [Apache Calcite Documentation](https://calcite.apache.org/docs/)
- [OpenSearch Plugin Development](https://opensearch.org/docs/latest/install-and-configure/plugins/)
- [OpenSearch Query DSL](https://opensearch.org/docs/latest/query-dsl/)
