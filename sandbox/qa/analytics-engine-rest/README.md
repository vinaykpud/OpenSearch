# Analytics Engine REST Integration Tests

REST-based integration tests for the analytics engine, running against a live OpenSearch cluster.

## Test Classes

| Test | Backend | Description |
|------|---------|-------------|
| `DataFusionScanRestIT` | DataFusion | End-to-end PPL queries through the DataFusion native execution path using mock parquet data |
| `LuceneScanRestIT` | Lucene | PPL queries through the Lucene scan path |
| `ClickBenchRestIT` | DataFusion | ClickBench workload queries |

## Prerequisites

### OpenSearch Core

Publish OpenSearch core to your local Maven repository:

```bash
./gradlew publishToMavenLocal -x test -x javadoc
```

### SQL Plugin

The analytics engine depends on the SQL plugin built from the `feature/mustang-ppl-integration` branch. Clone and publish it to your local Maven repository:

```bash
git clone -b feature/mustang-ppl-integration https://github.com/vinaykpud/sql.git
cd sql
./gradlew publishToMavenLocal -x test -x javadoc
```

## Running DataFusionScanRestIT

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
