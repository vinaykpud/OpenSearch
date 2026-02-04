# Query Planner Plugin

A distributed query execution engine for OpenSearch using Apache Calcite plans.

Todos:
1. Need to handle expressions, right now just hard coded throughout
2. Better handle jni interaction / plan building at df side, explore substrait conversion for subplans
3. merge with our feature branch / searcher code
4. Streaming transport / batching back and feed batches to datafusion
5. All in one plugin, need plugin points & extensibility by sql plugin to use the coordinator, engine-datafusion to provide subplan executor implementation
6. lucene queries / filters aren't differentiated from regular, doc values scan just does a match all at the moment
7. merging isn't happening at coordinator yet...

## Architecture Overview

```
SQL Query
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│  CalciteSqlParser                                               │
│  SQL → Calcite LogicalPlan (RelNode)                            │
└─────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│  QueryPlanner                                                   │
│  Logical optimization + partition analysis                      │
└─────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│  PhysicalOptimizer                                              │
│  LogicalPlan → Physical RelNode tree (OpenSearchRel)            │
│  - Converts to OpenSearch convention                            │
│  - Inserts Exchange nodes for distribution                      │
│  - Wraps subtrees in NativeEngine for DataFusion execution      │
└─────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│  RelToExecConverter                                             │
│  Physical RelNode → ExecNode tree (serializable)                │
└─────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│  QueryScheduler                                                 │
│  - Builds stages at Exchange boundaries (via StageBuilder)      │
│  - Dispatches LEAF stage to shards                              │
│  - Gathers results and executes ROOT stage on coordinator       │
│  - Returns CompletableFuture<VectorSchemaRoot>                  │
└─────────────────────────────────────────────────────────────────┘
    │
    ▼
Arrow VectorSchemaRoot (columnar results)
```

## Key Interfaces

### Query Coordination Layer

| Interface | Description |
|-----------|-------------|
| `QueryCoordinator` | Main entry point. Takes logical plan, returns Arrow results. |
| `QueryPlanner` | Logical optimization and partition analysis. |
| `PhysicalOptimizer` | Converts logical → physical plan using Calcite VolcanoPlanner. |

### Scheduling Layer

| Interface | Description |
|-----------|-------------|
| `QueryScheduler` | Schedules and executes ExecNode plans. Returns `CompletableFuture<VectorSchemaRoot>`. |
| `StageBuilder` | Internal: builds stages at Exchange boundaries. |
| `QueryStage` | Represents a stage (LEAF runs on shards, ROOT on coordinator). |

### Physical Plan Layer

| Interface | Description |
|-----------|-------------|
| `OpenSearchRel` | Base interface for physical RelNode operators. |
| `ExecNode` | Serializable execution plan nodes. Wire format for shard dispatch. |
| `Operator` | Volcano-style runtime operators. `open()`, `next()`, `close()`. |

### Native Engine Layer

| Interface | Description |
|-----------|-------------|
| `NativeEngineExecutor` | Executes ExecNode subtree via native engine. |
| `DataFusionExecutor` | DataFusion implementation via JNI. |
| `DataFusionBridge` | JNI native method declarations for DataFusion. |

## Data Flow

### Distributed Query (with aggregation)

```
Input: SELECT category, SUM(amount) FROM orders GROUP BY category

1. Logical Plan:
   LogicalAggregate(group=[category], SUM(amount))
     └── LogicalTableScan(orders)

2. Physical Plan (after PhysicalOptimizer):
   OpenSearchAggregate(FINAL)
     └── OpenSearchExchange(GATHER)
           └── OpenSearchAggregate(PARTIAL)
                 └── OpenSearchNativeEngine
                       └── OpenSearchNativeScan(orders)

3. ExecNode Tree (serializable):
   ExecAggregate(FINAL)
     └── ExecExchange(GATHER)
           └── ExecAggregate(PARTIAL)
                 └── ExecEngine
                       └── ExecNativeScan(orders)

4. After Scheduling:
   LEAF stage: ExecAggregate(PARTIAL) → ExecEngine → ExecNativeScan
   ROOT stage: ExecAggregate(FINAL)
   Shards: [shard-0, shard-1, ...]

5. Execution:
   - LEAF dispatched to each shard
   - Each shard: Lucene filter → DocValues → DataFusion → partial aggs
   - Results gathered to coordinator
   - ROOT executes FINAL aggregation
   - Arrow VectorSchemaRoot returned
```

### Scan Model

Data flows from Lucene to the native engine (DataFusion):

```
Lucene Index
    │
    ▼ (optional filter pushdown)
Lucene Query → Doc IDs
    │
    ▼
DocValues Reader
    │
    ▼
Arrow Vectors (columnar data)
    │
    ▼
DataFusion (filter, project, aggregate, sort, limit)
    │
    ▼
Arrow VectorSchemaRoot (results)
```

## Package Structure

```
org.opensearch.queryplanner
├── coordinator/          # Query coordination (QueryCoordinator)
├── planner/              # Logical planning (QueryPlanner)
├── optimizer/            # Physical optimization (PhysicalOptimizer)
│   └── rules/            # Calcite converter rules
├── physical/
│   ├── rel/              # Physical RelNode operators (OpenSearchScan, etc.)
│   ├── exec/             # ExecNode serializable plan nodes
│   └── operator/         # Runtime Volcano operators
├── scheduler/            # Distributed scheduling (QueryScheduler, StageBuilder)
├── engine/
│   └── datafusion/       # DataFusion JNI integration
└── action/               # Transport actions for shard dispatch
    └── rest/             # REST API handlers
```

## Usage Example

```java
// Create coordinator
QueryCoordinator coordinator = new DefaultQueryCoordinator(
    transportService, clusterService, indicesService, allocator);

// Parse SQL
RelNode logicalPlan = CalciteSqlParser.parse(sql, schema);

// Execute (async)
coordinator.execute(logicalPlan)
    .thenAccept(root -> {
        // Process Arrow columnar data
        processResults(root);
        root.close();
    });
```

### Scheduling and Executing

```java
QueryScheduler scheduler = new DefaultQueryScheduler(
    transportService, clusterService, indicesService);
SchedulerContext context = new SchedulerContext(timeout, allocator);

// Schedule and execute asynchronously
scheduler.schedule(execNode, context)
    .thenAccept(result -> {
        // process result
        result.close();
    });
```

### Native Library (DataFusion JNI) // TODO merge with feature/datafusion, this is just hardcoded data

```bash
cd plugins/query-planner/jni
cargo build --release
```

The native library must be on `java.library.path` for DataFusion execution.

## REST API

```bash
# Execute SQL query
POST /_plugins/query_planner/sql
{
  "query": "SELECT category, SUM(amount) FROM orders GROUP BY category"
}
```
