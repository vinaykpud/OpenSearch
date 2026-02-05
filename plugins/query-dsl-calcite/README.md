# OpenSearch DSL to Calcite Converter Plugin

A proof-of-concept OpenSearch plugin that converts OpenSearch Query DSL to Apache Calcite logical plans (RelNode). This plugin demonstrates how OpenSearch queries can be represented as relational algebra for potential query optimization and execution by alternative query engines.

## Overview

This plugin integrates with OpenSearch's search pipeline to convert DSL queries into Calcite's logical plan representation. It supports a wide range of OpenSearch query features including:

- **Query Types**: Term, Range, Match, Bool queries
- **Aggregations**: Metric aggregations (avg, sum, min, max, count) and bucket aggregations (terms)
- **Sorting**: Pre-aggregation and post-aggregation sorting
- **Pagination**: Offset and fetch (limit)
- **Projection**: Source filtering (_source includes)
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

### Key Components

- **DslCalcitePlugin**: Main plugin class implementing `DslConverterPlugin` interface
- **CalciteConverterService**: Service managing Calcite schema and converter instances
- **CalciteConverterImpl**: Core converter logic transforming DSL to RelNode
- **QueryBuilderVisitor**: Visitor pattern for converting query builders to RexNode
- **AggregationBuilderVisitor**: Visitor pattern for converting aggregation builders to AggregateCall
- **IndexMappingClient**: Retrieves and flattens OpenSearch index mappings
- **OpenSearchTypeMapper**: Maps OpenSearch field types to Calcite SQL types

### SearchSourceBuilder Components

The `SearchSourceBuilder` is the main DSL container. The plugin converts its components to Calcite RelNodes:

| Component | Purpose | Supported (Now) | Calcite Mapping |
|-----------|---------|-----------|-----------------|
| `query` | Row filtering | ✅ | `LogicalFilter` |
| `postFilter` | Post-aggregation filtering | ❌ | `LogicalFilter` after `LogicalAggregate` |
| `aggregations` | Grouping and metrics | ✅ | `LogicalAggregate` |
| `sorts` | Result ordering | ✅ | `LogicalSort` |
| `from`/`size` | Pagination | ✅ | `LogicalSort` with `offset`/`fetch` |
| `fetchSource` | Column projection | ✅ | `LogicalProject` |
| `scriptFields` | Computed fields | ❌ | `LogicalProject` with expressions |
| `highlight` | Result highlighting | ❌ | Not applicable |
| `suggest` | Search suggestions | ❌ | Not applicable |
| `rescore` | Result rescoring | ❌ | Not applicable |

## Supported Features

### QueryBuilder Hierarchy

The plugin converts OpenSearch QueryBuilder types to Calcite relational expressions:

#### Compound Queries

| Query Type | Supported (Now) | Calcite Representation |
|------------|-----------|------------------------|
| `BoolQueryBuilder` (must) | ✅ | `AND(condition1, condition2, ...)` - Native SQL |
| `BoolQueryBuilder` (filter) | ✅ | `AND(condition1, condition2, ...)` - Native SQL |
| `BoolQueryBuilder` (should) | ❌ | `OR(condition1, condition2, ...)` - Native SQL |
| `BoolQueryBuilder` (mustNot) | ❌ | `NOT(condition)` - Native SQL |
| `BoostingQueryBuilder` | ❌ | `BOOSTING(positive, negative, negative_boost)` - UDF |
| `ConstantScoreQueryBuilder` | ❌ | `CONSTANT_SCORE(filter, boost)` - UDF |
| `DisMaxQueryBuilder` | ❌ | `DIS_MAX(queries[], tie_breaker)` - UDF |
| `FunctionScoreQueryBuilder` | ❌ | `FUNCTION_SCORE(query, functions[], score_mode, boost_mode)` - UDF |
| `HybridQueryBuilder` | ❌ | `HYBRID(queries[], normalization, combination)` - UDF |

#### Term-Level Queries

| Query Type | Supported (Now) | Calcite Representation |
|------------|-----------|------------------------|
| `TermQueryBuilder` | ✅ | `=($field, 'value')` - Native SQL |
| `TermsQueryBuilder` | ❌ | `IN($field, value1, value2, ...)` - Native SQL |
| `TermsSetQueryBuilder` | ❌ | `TERMS_SET($field, values[], minimum_should_match)` - UDF |
| `IdsQueryBuilder` | ❌ | `IN($_id, id1, id2, ...)` - Native SQL |
| `RangeQueryBuilder` | ✅ | `AND(>=($field, min), <=($field, max))` - Native SQL |
| `PrefixQueryBuilder` | ❌ | `LIKE($field, 'prefix%')` - Native SQL |
| `ExistsQueryBuilder` | ❌ | `IS NOT NULL($field)` - Native SQL |
| `FuzzyQueryBuilder` | ❌ | `FUZZY($field, value, fuzziness, prefix_length)` - UDF |
| `WildcardQueryBuilder` | ❌ | `LIKE($field, 'pattern')` - Native SQL |
| `RegexpQueryBuilder` | ❌ | `RLIKE($field, 'pattern')` - Native SQL |

#### Full-Text Queries

| Query Type | Supported (Now) | Calcite Representation |
|------------|-----------|------------------------|
| `MatchQueryBuilder` | ✅ | `MATCH_QUERY($field, 'text', 'operator')` - UDF |
| `MatchPhraseQueryBuilder` | ❌ | `MATCH_PHRASE($field, 'phrase', slop)` - UDF |
| `MatchPhrasePrefixQueryBuilder` | ❌ | `MATCH_PHRASE_PREFIX($field, 'text', max_expansions)` - UDF |
| `MatchBoolPrefixQueryBuilder` | ❌ | `MATCH_BOOL_PREFIX($field, 'text')` - UDF |
| `MultiMatchQueryBuilder` | ❌ | `MULTI_MATCH(query, fields[], type)` - UDF |
| `QueryStringQueryBuilder` | ❌ | `QUERY_STRING(query, default_field)` - UDF |
| `SimpleQueryStringQueryBuilder` | ❌ | `SIMPLE_QUERY_STRING(query, fields[])` - UDF |
| `CombinedFieldsQueryBuilder` | ❌ | `COMBINED_FIELDS(query, fields[])` - UDF |
| `IntervalsQueryBuilder` | ❌ | `INTERVALS($field, rule)` - UDF |
| `MatchAllQueryBuilder` | ✅ | `true` - Native SQL Literal |
| `MatchNoneQueryBuilder` | ❌ | `false` - Native SQL Literal |

#### Geographic Queries

| Query Type | Supported (Now) | Calcite Representation |
|------------|-----------|------------------------|
| `GeoBoundingBoxQueryBuilder` | ❌ | `GEO_BOUNDING_BOX($field, top_left, bottom_right)` - UDF |
| `GeoDistanceQueryBuilder` | ❌ | `GEO_DISTANCE($field, distance, location)` - UDF |
| `GeoPolygonQueryBuilder` | ❌ | `GEO_POLYGON($field, points[])` - UDF |
| `GeoShapeQueryBuilder` | ❌ | `GEO_SHAPE($field, shape, relation)` - UDF |
| `XYShapeQueryBuilder` | ❌ | `XY_SHAPE($field, shape, relation)` - UDF |

#### Span Queries

| Query Type | Supported (Now) | Calcite Representation |
|------------|-----------|------------------------|
| `SpanTermQueryBuilder` | ❌ | `SPAN_TERM($field, value)` - UDF |
| `SpanNearQueryBuilder` | ❌ | `SPAN_NEAR(clauses[], slop, in_order)` - UDF |
| `SpanFirstQueryBuilder` | ❌ | `SPAN_FIRST(match, end)` - UDF |
| `SpanOrQueryBuilder` | ❌ | `SPAN_OR(clauses[])` - UDF |
| `SpanNotQueryBuilder` | ❌ | `SPAN_NOT(include, exclude, pre, post)` - UDF |
| `SpanContainingQueryBuilder` | ❌ | `SPAN_CONTAINING(big, little)` - UDF |
| `SpanWithinQueryBuilder` | ❌ | `SPAN_WITHIN(big, little)` - UDF |
| `SpanMultiTermQueryBuilder` | ❌ | `SPAN_MULTI(match)` - UDF |

#### Joining Queries

| Query Type | Supported (Now) | Calcite Representation |
|------------|-----------|------------------------|
| `NestedQueryBuilder` | ❌ | `NESTED(path, query, score_mode)` - UDF |
| `HasChildQueryBuilder` | ❌ | `HAS_CHILD(type, query, score_mode)` - UDF |
| `HasParentQueryBuilder` | ❌ | `HAS_PARENT(parent_type, query, score)` - UDF |
| `ParentIdQueryBuilder` | ❌ | `PARENT_ID(type, id)` - UDF |

#### Specialized Queries

| Query Type | Supported (Now) | Calcite Representation |
|------------|-----------|------------------------|
| `DistanceFeatureQueryBuilder` | ❌ | `DISTANCE_FEATURE($field, origin, pivot)` - UDF |
| `MoreLikeThisQueryBuilder` | ❌ | `MORE_LIKE_THIS(fields[], like, config)` - UDF |
| `PercolateQueryBuilder` | ❌ | `PERCOLATE($field, document, index)` - UDF |
| `RankFeatureQueryBuilder` | ❌ | `RANK_FEATURE($field, function)` - UDF |
| `ScriptQueryBuilder` | ❌ | `SCRIPT(source, params)` - UDF |
| `ScriptScoreQueryBuilder` | ❌ | `SCRIPT_SCORE(query, script)` - UDF |
| `WrapperQueryBuilder` | ❌ | Unwraps contained query - N/A |
| `KnnQueryBuilder` | ❌ | `KNN($field, vector, k)` - UDF |
| `NeuralQueryBuilder` | ❌ | `NEURAL($field, query_text, model_id)` - UDF |
| `NeuralSparseQueryBuilder` | ❌ | `NEURAL_SPARSE($field, query_text, model_id)` - UDF |
| `PinnedQueryBuilder` | ❌ | `PINNED(ids[], organic_query)` - UDF |
| `RuleQueryBuilder` | ❌ | `RULE_QUERY(organic_query, ruleset_ids)` - UDF |
| `ShapeQueryBuilder` | ❌ | `SHAPE($field, shape, relation)` - UDF |

### Aggregations

#### Bucket Aggregations (GROUP BY equivalent)

| Aggregation Type | Supported (Now) | Calcite Representation |
|------------------|-----------|------------------------|
| `TermsAggregationBuilder` | ✅ | `GROUP BY $field` - Native SQL |
| `HistogramAggregationBuilder` | ❌ | `GROUP BY FLOOR($field / interval) * interval` - Native SQL |
| `DateHistogramAggregationBuilder` | ❌ | `GROUP BY DATE_TRUNC($field, interval)` - Native SQL |
| `AutoDateHistogramAggregationBuilder` | ❌ | `AUTO_DATE_HISTOGRAM($field, buckets)` - UDF |
| `RangeAggregationBuilder` | ❌ | `GROUP BY CASE WHEN` expressions - Native SQL |
| `DateRangeAggregationBuilder` | ❌ | `DATE_RANGE($field, ranges[], format)` - UDF |
| `IpRangeAggregationBuilder` | ❌ | `IP_RANGE($field, ranges[])` - UDF |
| `FilterAggregationBuilder` | ❌ | `FILTER_AGG(query)` - UDF |
| `FiltersAggregationBuilder` | ❌ | `FILTERS_AGG(filters_map)` - UDF |
| `GlobalAggregationBuilder` | ❌ | `GLOBAL()` - UDF (ignores query scope) |
| `MissingAggregationBuilder` | ❌ | `MISSING($field)` - UDF |
| `NestedAggregationBuilder` | ❌ | `NESTED_AGG(path)` - UDF |
| `ReverseNestedAggregationBuilder` | ❌ | `REVERSE_NESTED(path)` - UDF |
| `ChildrenAggregationBuilder` | ❌ | `CHILDREN(type)` - UDF |
| `ParentAggregationBuilder` | ❌ | `PARENT(type)` - UDF |
| `SamplerAggregationBuilder` | ❌ | `SAMPLER(shard_size)` - UDF |
| `DiversifiedSamplerAggregationBuilder` | ❌ | `DIVERSIFIED_SAMPLER($field, shard_size, max_docs_per_value)` - UDF |
| `SignificantTermsAggregationBuilder` | ❌ | `SIGNIFICANT_TERMS($field, background_filter)` - UDF |
| `SignificantTextAggregationBuilder` | ❌ | `SIGNIFICANT_TEXT($field, filter_duplicate_text)` - UDF |
| `RareTermsAggregationBuilder` | ❌ | `RARE_TERMS($field, max_doc_count)` - UDF |
| `GeoDistanceAggregationBuilder` | ❌ | `GEO_DISTANCE_AGG($field, origin, ranges[])` - UDF |
| `GeohashGridAggregationBuilder` | ❌ | `GEOHASH_GRID($field, precision)` - UDF |
| `GeotileGridAggregationBuilder` | ❌ | `GEOTILE_GRID($field, precision)` - UDF |
| `GeohexGridAggregationBuilder` | ❌ | `GEOHEX_GRID($field, precision)` - UDF |
| `AdjacencyMatrixAggregationBuilder` | ❌ | `ADJACENCY_MATRIX(filters_map)` - UDF |
| `MultiTermsAggregationBuilder` | ❌ | `MULTI_TERMS(terms[])` - UDF |
| `CompositeAggregationBuilder` | ❌ | `COMPOSITE(sources[], size, after)` - UDF |

#### Metric Aggregations (Aggregate functions)

| Aggregation Type | Supported (Now) | Calcite Representation |
|------------------|-----------|------------------------|
| `AvgAggregationBuilder` | ✅ | `AVG($field)` - Native SQL |
| `SumAggregationBuilder` | ✅ | `SUM($field)` - Native SQL |
| `MinAggregationBuilder` | ✅ | `MIN($field)` - Native SQL |
| `MaxAggregationBuilder` | ✅ | `MAX($field)` - Native SQL |
| `CountAggregationBuilder` | ✅ (implicit) | `COUNT()` - Native SQL |
| `ValueCountAggregationBuilder` | ❌ | `COUNT($field)` - Native SQL |
| `CardinalityAggregationBuilder` | ❌ | `COUNT(DISTINCT $field)` - Native SQL |
| `StatsAggregationBuilder` | ❌ | Multiple: `COUNT`, `MIN`, `MAX`, `AVG`, `SUM` - Native SQL |
| `ExtendedStatsAggregationBuilder` | ❌ | Stats + variance, std_deviation, sum_of_squares - UDF |
| `MatrixStatsAggregationBuilder` | ❌ | `MATRIX_STATS(fields[])` - UDF (covariance, correlation) |
| `PercentilesAggregationBuilder` | ❌ | `PERCENTILES($field, percents[])` - UDF |
| `PercentileRanksAggregationBuilder` | ❌ | `PERCENTILE_RANKS($field, values[])` - UDF |
| `GeoBoundsAggregationBuilder` | ❌ | `GEO_BOUNDS($field)` - UDF |
| `GeoCentroidAggregationBuilder` | ❌ | `GEO_CENTROID($field)` - UDF |
| `GeoLineAggregationBuilder` | ❌ | `GEO_LINE(point_field, sort_field)` - UDF |
| `TopHitsAggregationBuilder` | ❌ | `TOP_HITS(size, sort, _source)` - UDF |
| `ScriptedMetricAggregationBuilder` | ❌ | `SCRIPTED_METRIC(init, map, combine, reduce)` - UDF |
| `MedianAbsoluteDeviationAggregationBuilder` | ❌ | `MEDIAN_ABSOLUTE_DEVIATION($field)` - UDF |
| `WeightedAvgAggregationBuilder` | ❌ | `WEIGHTED_AVG(value_field, weight_field)` - UDF |
| `TTestAggregationBuilder` | ❌ | `T_TEST(field_a, field_b, type)` - UDF |
| `RateAggregationBuilder` | ❌ | `RATE($field, unit)` - UDF |
| `StringStatsAggregationBuilder` | ❌ | `STRING_STATS($field)` - UDF |

#### Pipeline Aggregations (Post-processing)

| Aggregation Type | Supported (Now) | Calcite Representation |
|------------------|-----------|------------------------|
| `AvgBucketAggregationBuilder` | ❌ | `AVG_BUCKET(buckets_path)` - UDF |
| `SumBucketAggregationBuilder` | ❌ | `SUM_BUCKET(buckets_path)` - UDF |
| `MinBucketAggregationBuilder` | ❌ | `MIN_BUCKET(buckets_path)` - UDF |
| `MaxBucketAggregationBuilder` | ❌ | `MAX_BUCKET(buckets_path)` - UDF |
| `StatsBucketAggregationBuilder` | ❌ | `STATS_BUCKET(buckets_path)` - UDF |
| `ExtendedStatsBucketAggregationBuilder` | ❌ | `EXTENDED_STATS_BUCKET(buckets_path)` - UDF |
| `PercentilesBucketAggregationBuilder` | ❌ | `PERCENTILES_BUCKET(buckets_path, percents[])` - UDF |
| `DerivativeAggregationBuilder` | ❌ | `DERIVATIVE(buckets_path, unit)` - UDF |
| `CumulativeSumAggregationBuilder` | ❌ | `CUMULATIVE_SUM(buckets_path)` - UDF |
| `CumulativeCardinalityAggregationBuilder` | ❌ | `CUMULATIVE_CARDINALITY(buckets_path)` - UDF |
| `MovingAvgAggregationBuilder` | ❌ | `MOVING_AVG(buckets_path, model, window)` - UDF |
| `MovingFunctionAggregationBuilder` | ❌ | `MOVING_FN(buckets_path, script, window)` - UDF |
| `MovingPercentilesAggregationBuilder` | ❌ | `MOVING_PERCENTILES(buckets_path, window)` - UDF |
| `SerialDifferencingAggregationBuilder` | ❌ | `SERIAL_DIFF(buckets_path, lag)` - UDF |
| `BucketScriptAggregationBuilder` | ❌ | `BUCKET_SCRIPT(buckets_path_map, script)` - UDF |
| `BucketSelectorAggregationBuilder` | ❌ | `BUCKET_SELECTOR(buckets_path_map, script)` - UDF |
| `BucketSortAggregationBuilder` | ❌ | `BUCKET_SORT(sort[], from, size)` - UDF |
| `NormalizeAggregationBuilder` | ❌ | `NORMALIZE(buckets_path, method)` - UDF |

### Search Features

Additional OpenSearch search features and their Calcite representations:

| Feature | Supported (Now) | Calcite Representation |
|---------|-----------|------------------------|
| **Sorting** | ✅ | `LogicalSort` with collations - Native Calcite RelNode |
| Post-aggregation sorting | ✅ | `LogicalSort` after `LogicalAggregate` - Native Calcite RelNode |
| Geo distance sort | ❌ | `GEO_DISTANCE_SORT($field, location)` - UDF |
| Script sort | ❌ | `SCRIPT_SORT(script)` - UDF |
| Nested sort | ❌ | `NESTED_SORT(path, filter)` - UDF |
| **Pagination** | ✅ | `LogicalSort` with offset/fetch - Native Calcite RelNode |
| search_after | ❌ | `SEARCH_AFTER(sort_values[])` - UDF |
| Point in Time (PIT) | ❌ | `POINT_IN_TIME(pit_id, keep_alive)` - UDF |
| **Projection** | ✅ | `LogicalProject` - Native Calcite RelNode |
| Source filtering (_source) | ✅ | Field selection in `LogicalProject` - Native Calcite RelNode |
| Stored fields | ❌ | `STORED_FIELDS(fields[])` - UDF |
| Doc value fields | ❌ | `DOCVALUE_FIELDS(fields[], format)` - UDF |
| Script fields | ❌ | `SCRIPT_FIELDS(field_name, script)` - UDF |
| Fields parameter | ❌ | `FIELDS(patterns[], format)` - UDF |
| **Result Processing** | | |
| Highlight | ❌ | `HIGHLIGHT(fields[], config)` - UDF |
| Collapse | ❌ | `COLLAPSE($field, inner_hits)` - UDF |
| Rescore | ❌ | `RESCORE(query, window_size, weights)` - UDF |
| Inner hits | ❌ | `INNER_HITS(name, config)` - UDF |
| **Suggestions** | | |
| Term suggester | ❌ | `TERM_SUGGEST($field, text, mode)` - UDF |
| Phrase suggester | ❌ | `PHRASE_SUGGEST($field, text, config)` - UDF |
| Completion suggester | ❌ | `COMPLETION_SUGGEST($field, prefix, fuzzy)` - UDF |
| Context suggester | ❌ | `CONTEXT_SUGGEST($field, prefix, contexts)` - UDF |
| **Execution Control** | | |
| Timeout | ❌ | Metadata: max execution time |
| Terminate after | ❌ | Metadata: max docs per shard |
| Min score | ❌ | `LogicalFilter` with score threshold - Native Calcite RelNode |
| Track total hits | ❌ | Metadata: count accuracy |
| Track scores | ❌ | Metadata: force scoring |
| Explain | ❌ | Metadata: return score explanation |
| Profile | ❌ | Metadata: return timing info |
| **Advanced Parameters** | | |
| Indices boost | ❌ | `INDICES_BOOST(index_boost_pairs)` - UDF |
| Runtime mappings | ❌ | Runtime field definitions - Metadata |
| Minimum should match | ❌ | Preserved in bool/full-text UDFs - Parameter |
| Rewrite parameter | ❌ | Preserved in multi-term query UDFs - Parameter |

## Usage

The plugin automatically integrates with OpenSearch's search pipeline. When a search request is executed, the plugin converts the DSL query to a Calcite logical plan and logs the result.

### Example Queries

#### 1. Term Query

**DSL:**
```json
{
  "query": {
    "term": {
      "category": "electronics"
    }
  }
}
```

**Calcite Plan:**
```
LogicalFilter(condition=[=($0, 'electronics')])
  LogicalTableScan(table=[[my-index]])
```

#### 2. Range Query

**DSL:**
```json
{
  "query": {
    "range": {
      "price": {
        "gte": 100,
        "lte": 500
      }
    }
  }
}
```

**Calcite Plan:**
```
LogicalFilter(condition=[AND(>=($0, 100), <=($0, 500))])
  LogicalTableScan(table=[[my-index]])
```

#### 3. Bool Query with Aggregation

**DSL:**
```json
{
  "query": {
    "bool": {
      "must": [
        { "match": { "title": "laptop" } }
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
        "avg_price": {
          "avg": { "field": "price" }
        }
      }
    }
  },
  "sort": [
    { "price": { "order": "asc" } }
  ]
}
```

**Calcite Plan:**
```
LogicalAggregate(group=[{1}], avg_price=[AVG($2)], _count=[COUNT()])
  LogicalSort(sort0=[$2], dir0=[ASC])
    LogicalFilter(condition=[AND(MATCH_QUERY($3, 'laptop', 'OR'), =($0, 'electronics'), >=($2, 500), <=($2, 2000))])
      LogicalTableScan(table=[[my-index]])
```

## Type Mappings

OpenSearch field types are mapped to Calcite SQL types:

| OpenSearch Type | Calcite Type |
|-----------------|--------------|
| keyword, text | VARCHAR |
| long | BIGINT |
| integer | INTEGER |
| short | SMALLINT |
| byte | TINYINT |
| double, scaled_float | DOUBLE |
| float, half_float | FLOAT |
| boolean | BOOLEAN |
| date, date_nanos | TIMESTAMP |
| binary | VARBINARY |
| ip | VARCHAR |
| object, nested | ANY (flattened) |

## Building

Build the plugin:

```bash
./gradlew :plugins:query-dsl-calcite:assemble
```

The plugin ZIP will be created at:
```
plugins/query-dsl-calcite/build/distributions/query-dsl-calcite-<version>.zip
```

## Installation

### Development Mode

Run OpenSearch with the plugin installed:

```bash
./gradlew run -PinstalledPlugins="['query-dsl-calcite']"
```

### Production Installation

Install the plugin ZIP:

```bash
bin/opensearch-plugin install file:///path/to/query-dsl-calcite-<version>.zip
```

## Testing

### Unit Tests

Run unit tests:

```bash
./gradlew :plugins:query-dsl-calcite:test
```

### Integration Tests

Run integration tests:

```bash
./gradlew :plugins:query-dsl-calcite:internalClusterTest
```

Run specific test:

```bash
./gradlew :plugins:query-dsl-calcite:internalClusterTest --tests "DslCalciteIntegrationIT.testComplexQueryConversion"
```

## Limitations

This is a proof-of-concept implementation with the following limitations:

1. **Read-Only**: The plugin only converts queries to logical plans; it does not execute them
2. **Logging Only**: Converted plans are logged but not used for query execution
3. **Limited Query Types**: Only supports a subset of OpenSearch query types
4. **No Nested Objects**: Nested objects are flattened using dot notation
5. **No Script Fields**: Script-based queries and aggregations are not supported
6. **Pagination with Aggregations**: Pagination (offset/fetch) is not applied when aggregations are present

## Dependencies

- **Apache Calcite Core**: 1.38.0
- **Apache Calcite Linq4j**: 1.38.0
- **Guava**: 33.4.0-jre
- **Commons Lang3**: 3.17.0

## Future Enhancements

Potential areas for enhancement:

1. **Query Execution**: Execute Calcite plans using Calcite's execution engine
2. **Query Optimization**: Apply Calcite's rule-based optimizer
3. **Additional Query Types**: Support for more complex queries (nested, geo, etc.)
4. **Custom Functions**: Implement OpenSearch-specific functions as Calcite UDFs
5. **Cost-Based Optimization**: Integrate with OpenSearch statistics for cost estimation
6. **Distributed Execution**: Coordinate execution across OpenSearch cluster nodes

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
