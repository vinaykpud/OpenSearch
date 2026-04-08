# Analytics Engine REST Integration Tests

REST-based integration tests for the analytics engine, running against a live OpenSearch cluster.

## Test Classes

| Test | Backend | Description |
|------|---------|-------------|
| `DataFusionScanRestIT` | DataFusion | End-to-end PPL queries through the DataFusion native execution path using mock parquet data |
| `DslDataFusionRestIT` | DataFusion | DSL `_search` queries intercepted by the dsl-query-executor plugin, executed through the DataFusion backend |
| `LuceneScanRestIT` | Lucene | PPL queries through the Lucene scan path |
| `ClickBenchRestIT` | DataFusion | ClickBench workload queries |

## Running DataFusionScanRestIT

### Prerequisites

#### OpenSearch Core

Publish OpenSearch core to your local Maven repository:

```bash
./gradlew publishToMavenLocal -x test -x javadoc
```

#### SQL Plugin

The analytics engine depends on the SQL plugin built from the `feature/mustang-ppl-integration` branch: https://github.com/vinaykpud/sql/commits/feature/mustang-ppl-integration/

Clone and publish it to your local Maven repository:

```bash
git clone -b feature/mustang-ppl-integration https://github.com/vinaykpud/sql.git
cd sql
./gradlew publishToMavenLocal -x test -x javadoc
```

### 1. Build the plugins

```bash
./gradlew :sandbox:plugins:analytics-engine:assemble :sandbox:plugins:analytics-backend-datafusion:assemble -x missingJavadoc
```

### 2. Start the cluster

```bash
./gradlew run -PinstalledPlugins="['opensearch-job-scheduler', 'opensearch-sql-plugin', 'analytics-engine', 'analytics-backend-datafusion']"
```

Wait until the cluster is ready (logs show `started`).

### 3. Run the test

In a separate terminal:

```bash
./gradlew :sandbox:qa:analytics-engine-rest:restTest -Dtests.class=org.opensearch.analytics.qa.DataFusionScanRestIT
```

### Queries

The test runs the following PPL queries against the `parquet_simple` index and validates the results from the mock parquet data (100 rows):

| Query | PPL | Expected |
|-------|-----|----------|
| q1 | `stats count() as cnt` | cnt = 100 |
| q2 | `stats sum(age) as total_age` | total_age = 4228 |
| q3 | `stats count() as cnt by city` | paris=12, tokyo=22, berlin=26, new york=22, london=18 |
| q4 | `stats min(age) as min_age, max(age) as max_age` | min=18, max=65 |
| q5 | `fields name, city` | 100 rows, 2 columns |

### Notes

- **No data ingestion needed.** The DataFusion backend uses a mock parquet reader that serves 100 rows of pre-generated data (id, name, age, score, city). The test only creates the index for schema registration.
- The test exercises the full pipeline: PPL parsing, Calcite planning, backend marking, DAG construction, Substrait fragment conversion, shard dispatch, DataFusion native execution, and Arrow result collection.

## Running DslDataFusionRestIT

### 1. Build the plugins

```bash
./gradlew :sandbox:plugins:analytics-engine:assemble :sandbox:plugins:analytics-backend-datafusion:assemble :sandbox:plugins:dsl-query-executor:assemble -x javadoc -x test
```

### 2. Start the cluster

```bash
./gradlew run -PinstalledPlugins="['analytics-engine', 'analytics-backend-datafusion', 'dsl-query-executor']"
```

Wait until the cluster is ready (logs show `started`).

### 3. Run the test

```bash
./gradlew :sandbox:qa:analytics-engine-rest:restTest --tests "org.opensearch.analytics.qa.DslDataFusionRestIT"
```

### Queries

The test sends DSL queries to `/_search` which are intercepted by the `dsl-query-executor` plugin and executed through the Calcite → DataFusion pipeline:

| Query | DSL | Validates |
|-------|-----|-----------|
| q1 | `match_all` | Pipeline returns 200, hits present, no timeout |
| q2 | `match_all` with `size: 5` | Pipeline handles size parameter |
| q3 | `term` filter on city | Pipeline handles filter queries |
| q4 | `terms` aggregation on city | **Disabled** — OpenSearchAggregateSplitRule type mismatch bug |
| q5 | `_source` projection | Pipeline handles source filtering |
| q6 | `sort` by age desc | Pipeline handles sort queries |

### Notes

- **No data ingestion needed.** The test creates the index for schema registration and the DataFusion backend uses a mock parquet reader.
- The `dsl-query-executor` plugin intercepts all `_search` requests with a non-null source body and routes them through: DSL interception → SearchSourceConverter → Calcite planning → DataFusion execution → SearchResponse.
- The `SearchSourceConverter` currently only builds a `LogicalTableScan` — filter, projection, aggregation, and sort conversion are not yet implemented. All queries return empty hits but confirm the pipeline executes without errors.

## Manual Testing — PPL Queries

Requires the same [prerequisites](#prerequisites) as DataFusionScanRestIT (OpenSearch Core + SQL Plugin).

### 1. Build the plugins

```bash
./gradlew :sandbox:plugins:analytics-engine:assemble :sandbox:plugins:analytics-backend-datafusion:assemble -x javadoc -x test
```

### 2. Start the cluster

```bash
./gradlew run -PinstalledPlugins="['opensearch-job-scheduler', 'opensearch-sql-plugin', 'analytics-engine', 'analytics-backend-datafusion']"
```

Wait until the cluster is ready (logs show `started`).

### 3. Create the index

```bash
curl -X PUT "http://localhost:9200/parquet_simple" -H "Content-Type: application/json" -d '{
  "settings": {"index.number_of_shards": 1, "index.number_of_replicas": 0},
  "mappings": {"properties": {"id": {"type": "long"}, "name": {"type": "keyword"}, "age": {"type": "long"}, "score": {"type": "long"}, "city": {"type": "keyword"}}}
}'
```

### 4. Run queries

```bash
# COUNT(*)
curl -X POST "http://localhost:9200/_plugins/_ppl" -H "Content-Type: application/json" \
  -d '{"query": "source=parquet_simple | stats count() as cnt"}'

# SUM(age)
curl -X POST "http://localhost:9200/_plugins/_ppl" -H "Content-Type: application/json" \
  -d '{"query": "source=parquet_simple | stats sum(age) as total_age"}'

# COUNT(*) GROUP BY city
curl -X POST "http://localhost:9200/_plugins/_ppl" -H "Content-Type: application/json" \
  -d '{"query": "source=parquet_simple | stats count() as cnt by city"}'

# MIN/MAX age
curl -X POST "http://localhost:9200/_plugins/_ppl" -H "Content-Type: application/json" \
  -d '{"query": "source=parquet_simple | stats min(age) as min_age, max(age) as max_age"}'

# Projection
curl -X POST "http://localhost:9200/_plugins/_ppl" -H "Content-Type: application/json" \
  -d '{"query": "source=parquet_simple | fields name, city"}'

# SUM(age) GROUP BY city
curl -X POST "http://localhost:9200/_plugins/_ppl" -H "Content-Type: application/json" \
  -d '{"query": "source=parquet_simple | stats sum(age) as total_age by city"}'
```

## Manual Testing — DSL Queries

### 1. Build the plugins

```bash
./gradlew :sandbox:plugins:analytics-engine:assemble :sandbox:plugins:analytics-backend-datafusion:assemble :sandbox:plugins:dsl-query-executor:assemble -x javadoc -x test
```

### 2. Start the cluster

```bash
./gradlew run -PinstalledPlugins="['analytics-engine', 'analytics-backend-datafusion', 'dsl-query-executor']"
```

Wait until the cluster is ready (logs show `started`).

### 3. Create the index

```bash
curl -X PUT "http://localhost:9200/parquet_simple" -H "Content-Type: application/json" -d '{
  "settings": {"index.number_of_shards": 1, "index.number_of_replicas": 0},
  "mappings": {"properties": {"id": {"type": "long"}, "name": {"type": "keyword"}, "age": {"type": "long"}, "score": {"type": "long"}, "city": {"type": "keyword"}}}
}'
```

### 4. Run queries

```bash
# match_all
curl -s "http://localhost:9200/parquet_simple/_search" -H "Content-Type: application/json" \
  -d '{"query": {"match_all": {}}}'

# match_all with size
curl -s "http://localhost:9200/parquet_simple/_search" -H "Content-Type: application/json" \
  -d '{"size": 5, "query": {"match_all": {}}}'

# term filter
curl -s "http://localhost:9200/parquet_simple/_search" -H "Content-Type: application/json" \
  -d '{"query": {"term": {"city": "paris"}}}'

# projection
curl -s "http://localhost:9200/parquet_simple/_search" -H "Content-Type: application/json" \
  -d '{"_source": ["name", "city"], "query": {"match_all": {}}}'

# sort
curl -s "http://localhost:9200/parquet_simple/_search" -H "Content-Type: application/json" \
  -d '{"sort": [{"age": "desc"}], "query": {"match_all": {}}}'
```

### Notes

- DSL queries are intercepted by the `dsl-query-executor` plugin and routed through the Calcite → DataFusion pipeline.
- DSL queries are executed through the full pipeline: DSL interception → Calcite planning → DataFusion execution → SearchResponse.
