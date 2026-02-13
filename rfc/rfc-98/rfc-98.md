<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# RFC-98: Spark Datasource V2 Read

## Proposers

- @geserdugarov

## Approvers

- @

## Status

Umbrella ticket: [HUDI-4449](https://issues.apache.org/jira/browse/HUDI-4449)

## Abstract

Data source is one of the foundational APIs in Spark, with two major versions known as "V1" and "V2". 
The representation of a read in the physical plan differs depending on the API version used.
Adopting the V2 API is essential for enhanced control over the data source, deeper integration with the Spark optimizer, and improved overall performance.

First steps towards integrating of Spark Datasource V2 were taken in [RFC-38](../rfc-38/rfc-38.md). 
However, there are multiple issues with advertising Hudi table as V2 without actual implementing certain API, and with using custom relation rule to fall back to V1 API.
As a result, the current implementation of `HoodieCatalog` and `Spark3DefaultSource` returns a `V1Table` instead of `HoodieInternalV2Table`, 
in order to [address performance regression](https://github.com/apache/hudi/pull/5737).

There was [an attempt](https://github.com/apache/hudi/pull/6442) to implement Spark Datasource V2 read functionality as a regular task, 
but it failed due to the scope of work required.
Therefore, this RFC proposes to discuss design of Spark Datasource V2 integration in advance and to continue working on it accordingly.

## Background

The current implementation of Spark Datasource V2 integration is presented in the schema below:

![Current integration with Spark](initial_integration_with_Spark.jpg)

### Lessons Learned from Previous Attempts

**RFC-38 / HUDI-4178 — Performance Regression.**
The initial DSv2 integration ([RFC-38](../rfc-38/rfc-38.md)) advertised Hudi tables as V2
without fully implementing the V2 read API. When Spark encounters a V2 table, it uses V2
planning rules with different codegen and execution characteristics. Without a complete V2
read path, the V2 planning overhead degraded performance compared to V1's `BaseRelation` +
`HoodieFileIndex` pipeline. [PR #5737](https://github.com/apache/hudi/pull/5737) fixed this
by making `HoodieCatalog.loadTable()` return a `V1Table` wrapper by default, effectively
downgrading reads back to V1.

**PR #6442 — Scope Failure.**
A subsequent attempt to implement the full DSv2 read in a single PR
([PR #6442](https://github.com/apache/hudi/pull/6442)) failed because the scope was too
large: read, filter pushdown, statistics, schema evolution, and columnar reads were all
attempted at once.

### Key Design Constraint: Preserve DSv1 Write Performance

The write path must remain completely unaffected. The existing `HoodieSparkSqlWriter` pipeline
(DSv1 `CreatableRelationProvider` → `SparkRDDWriteClient`) is the production write path.
Any DSv2 integration must be limited to the read side, keeping writes on DSv1.

## Implementation

### Design Principle: DSv1 Write / DSv2 Read Coexistence

The architecture supports DSv1 write and DSv2 read simultaneously through three mechanisms
already present in the codebase:

**1. `HoodieInternalV2Table` implements both interfaces independently.**

```
HoodieInternalV2Table
  extends Table
  with SupportsRead      ← DSv2 read (newScanBuilder → HoodieScanBuilder)
  with SupportsWrite     ← write (newWriteBuilder → HoodieV1WriteBuilder → V1Write)
  with V2TableWithV1Fallback  ← V1 fallback for reads when DSv2 is disabled
```

The `capabilities()` method declares `BATCH_READ` (V2 read) alongside `V1_BATCH_WRITE`
(V1 write). This tells Spark to use the V2 `ScanBuilder` for reads while routing writes
through the V1 `InsertableRelation` interface:

```scala
override def capabilities() = Set(
  BATCH_READ,          // DSv2 read
  V1_BATCH_WRITE,      // DSv1 write — routes through existing HoodieSparkSqlWriter
  OVERWRITE_BY_FILTER, TRUNCATE, ACCEPT_ANY_SCHEMA
)
```

**2. `HoodieV1WriteBuilder` delegates writes to the DSv1 path.**

The write builder produces a `V1Write` that internally calls `data.write.format("org.apache.hudi").save()`,
routing through the existing `DefaultSource.createRelation()` → `HoodieSparkSqlWriter.write()`
→ `SparkRDDWriteClient` pipeline. Write performance is identical to today because the
codepath is literally the same.

**3. `HoodieCatalog.loadTable()` gates the V2 read path.**

```scala
override def loadTable(ident: Identifier): Table = {
  val v2Table = HoodieInternalV2Table(...)
  val dsv2ReadEnabled = spark.conf.getOption("hoodie.datasource.read.use.dsv2")
    .exists(_.toBoolean)
  if (schemaEvolutionEnabled || dsv2ReadEnabled) {
    v2Table                // DSv2 read path
  } else {
    v2Table.v1TableWrapper // DSv1 read path (current default)
  }
}
```

When `hoodie.datasource.read.use.dsv2=false` (default), the table is wrapped as `V1Table`
and Spark uses the existing V1 read pipeline — zero behavior change. When enabled, Spark
receives the full `HoodieInternalV2Table` and uses its `ScanBuilder` for reads while still
routing writes through `V1Write`.

### Read

The DSv2 read path follows the standard Spark DataSource V2 interface chain,
modeled after Apache Iceberg's implementation:

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         DRIVER SIDE (Planning)                          │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  HoodieCatalog.loadTable()                                               │
│       │                                                                  │
│       ▼                                                                  │
│  HoodieInternalV2Table (SupportsRead)                                    │
│       │                                                                  │
│       │ newScanBuilder(options)                                          │
│       ▼                                                                  │
│  HoodieScanBuilder                                                       │
│       │                                                                  │
│       ├── pushFilters() ──► 3-way filter classification                  │
│       │       ├── Partition filters → HoodieFileIndex partition pruning   │
│       │       ├── Data filters → column stats / bloom / record index     │
│       │       └── Unsupported → returned to Spark for post-scan eval     │
│       │                                                                  │
│       ├── pruneColumns() ──► Column pruning                              │
│       │                                                                  │
│       │ build()                                                          │
│       ▼                                                                  │
│  HoodieScan (SupportsReportStatistics)                                   │
│       │                                                                  │
│       ├── estimateStatistics() ──► sizeInBytes, numRows for CBO          │
│       │                                                                  │
│       │ toBatch()                                                        │
│       │   ├── Build HoodieTableMetaClient                                │
│       │   ├── Create HoodieFileIndex (reuse existing Scala class)        │
│       │   ├── filterFileSlices(dataFilters, partitionFilters)            │
│       │   ├── Get latestCommitTime from timeline                         │
│       │   └── Create HoodieInputPartition per FileSlice                  │
│       ▼                                                                  │
│  HoodieBatch                                                             │
│       │                                                                  │
│       ├── planInputPartitions() ──► HoodieInputPartition[]               │
│       └── createReaderFactory() ──► HoodiePartitionReaderFactory         │
│                                     (Serializable, shipped to executors) │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│                       EXECUTOR SIDE (Reading)                            │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  HoodiePartitionReaderFactory                                            │
│       │                                                                  │
│       ├── createReader(partition)                                         │
│       │       └── HoodiePartitionReader ──► InternalRow iteration        │
│       │              ├── Build HoodieTableMetaClient (from SerConf)      │
│       │              ├── Build SparkFileFormatInternalRowReaderContext    │
│       │              ├── Build HoodieFileGroupReader (handles MOR merge) │
│       │              └── Iterate via getClosableIterator()               │
│       │                                                                  │
│       └── createColumnarReader(partition)  [COW only, no log files]      │
│               └── HoodieColumnarPartitionReader ──► ColumnarBatch        │
│                      └── Vectorized Parquet reader (sparkAdapter)        │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

#### DSv2 Class Chain

| Class | Spark Interface | Role |
|-------|----------------|------|
| `HoodieInternalV2Table` | `Table`, `SupportsRead` | Entry point; creates `HoodieScanBuilder` |
| `HoodieScanBuilder` | `ScanBuilder`, `SupportsPushDownFilters`, `SupportsPushDownRequiredColumns` | Accepts filter and column pushdowns from Spark optimizer |
| `HoodieScan` | `Scan`, `SupportsReportStatistics` | Driver-side file planning via `HoodieFileIndex` |
| `HoodieBatch` | `Batch` | Packages `HoodieInputPartition[]` and creates reader factory |
| `HoodieInputPartition` | `InputPartition` | Serializable work unit wrapping a `FileSlice` |
| `HoodiePartitionReaderFactory` | `PartitionReaderFactory` | Serializable factory shipped to executors |
| `HoodiePartitionReader` | `PartitionReader<InternalRow>` | Executor-side row reader using `HoodieFileGroupReader` |
| `HoodieColumnarPartitionReader` | `PartitionReader<ColumnarBatch>` | Executor-side vectorized reader for COW base files |

#### Filter Pushdown: 3-Way Classification

Following the Iceberg pattern, `HoodieScanBuilder.pushFilters()` classifies each Spark `Filter`
into three categories:

| Category | Evaluated By | Used For | Example |
|----------|-------------|----------|---------|
| Partition filter | HoodieFileIndex | Partition pruning | `WHERE partition_col = 'X'` |
| Data filter | HoodieFileIndex + Parquet reader | Column stats skipping, bloom filter, record index; Parquet predicate pushdown | `WHERE data_col > 100` |
| Unsupported | Spark (post-scan) | Returned from `pushFilters()` | `WHERE udf(col) > 10` |

Partition columns are identified via `HoodieTableConfig.getPartitionFields()`. Only
unsupported filters are returned from `pushFilters()` — partition and data filters are
stored internally and passed through to `HoodieScan.toBatch()` for file pruning
and to `HoodiePartitionReader` for Parquet predicate pushdown.

#### Statistics for Cost-Based Optimization

`HoodieScan.estimateStatistics()` returns:
- **sizeInBytes**: Sum of base file and log file sizes from all planned `FileSlice`s
- **numRows**: From latest commit metadata (`TOTAL_RECORDS_WRITTEN`) when available

File slice computation is lazy and cached — shared between `estimateStatistics()` and `toBatch()`.

#### Executor-Side Reading

`HoodiePartitionReader` delegates to `HoodieFileGroupReader`, the existing engine-agnostic
file group reader that handles both COW (base file only) and MOR (base file + log file merge).
The pattern follows `HoodieFileGroupReaderBasedFileFormat.buildReaderWithPartitionValues()`:

1. Build `HoodieTableMetaClient` from serialized config
2. Create `SparkFileFormatInternalRowReaderContext` (non-vectorized, for MOR merge support)
3. Build `HoodieFileGroupReader` via builder with file slice, schemas, and commit time
4. Obtain `ClosableIterator<InternalRow>` for row iteration

For COW file slices with no log files, `HoodiePartitionReaderFactory.supportColumnarReads()`
returns `true`, and `createColumnarReader()` uses Spark's native vectorized Parquet reader
for maximum performance.

#### Serialization

The `HoodiePartitionReaderFactory` must be `Serializable` for executor shipping. It carries:
- `SerializableConfiguration` (Hadoop config for `HoodieTableMetaClient` construction)
- `StructType` for table and required schemas
- `Map<String, String>` for Hudi properties
- `Filter[]` for data filters (Parquet predicate pushdown)

No references to `SparkSession`, `HoodieFileIndex`, or other driver-only objects.

### Table Services

Table services (compaction, clustering, cleaning) are **orthogonal** to the DSv2 read path.
They are triggered by the write client during or after writes (which remain on DSv1). The
DSv2 read path is a consumer of table state — it reads whatever file slices the timeline
exposes at planning time. No table service changes are required.

However, table services do affect what the DSv2 read path sees:

- **Compaction** (MOR): Merges log files into base files. After compaction, a file slice
  has only a base file, enabling columnar reads via `HoodieColumnarPartitionReader`.
- **Clustering**: Reorganizes data files. The DSv2 read path picks up clustered file groups
  through `HoodieFileIndex` transparently.
- **Cleaning**: Removes old file versions. The read path's snapshot isolation (planning-time
  timeline capture) ensures consistency — files are not cleaned while a read is in progress
  as long as the timeline instant is still active.

## Rollout/Adoption Plan

### Phase 1: End-to-End Read (PR 1)

Wire the two stub methods to make a basic DSv2 read work:
- `HoodieScan.toBatch()`: Build `HoodieFileIndex`, call `filterFileSlices()`, create
  `HoodieInputPartition` per file slice
- `HoodiePartitionReader.initialize()`: Build `HoodieFileGroupReader` via builder pattern

Scope: COW and MOR tables, partitioned and non-partitioned, basic column pruning.
No filter pushdown beyond what `HoodieFileIndex` does by default.

Gated behind `hoodie.datasource.read.use.dsv2=true` (default `false`).

### Phase 2: Filter Classification and Partition Pruning (PR 2)

Replace the stub `pushFilters()` in `HoodieScanBuilder` with proper 3-way filter
classification. Wire partition and data filters through to `HoodieFileIndex.filterFileSlices()`
and pass data filters to executor-side Parquet reader.

### Phase 3: Statistics for CBO (PR 3)

Implement `HoodieScan.estimateStatistics()` with actual file sizes and row counts.
Enable Spark's CBO to make informed decisions (e.g., broadcast joins for small tables).

### Phase 4: Schema Evolution (PR 4)

Resolve `InternalSchema` from commit metadata and pass it to `HoodieFileGroupReader.Builder`.
Handle schema changes across commits in the DSv2 read path.

### Phase 5: Columnar (Vectorized) Reads for COW (PR 5)

Enable vectorized Parquet reads for pure COW file slices (no log files).
`HoodiePartitionReaderFactory.supportColumnarReads()` returns `true` when the file slice
has only a base file; `createColumnarReader()` uses Spark's vectorized Parquet reader.

### Dependency Graph

```
Phase 1 (End-to-end read) ─────┬──► Phase 2 (Filter pushdown)
                                ├──► Phase 3 (Statistics)
                                ├──► Phase 4 (Schema evolution)
                                └──► Phase 5 (Columnar reads)
```

Phases 2-5 are independent of each other and can be developed in parallel after Phase 1.

### Performance Validation

Each phase must include benchmark comparison against the V1 read path:
- **Correctness**: DSv2 results must exactly match V1 results for the same query
- **Performance**: DSv2 must not regress compared to V1 for the same workload.
  The initial phases may be slower (row-based MOR reads) but Phase 5 (columnar COW)
  should match or exceed V1 performance.

### Activation

The feature is controlled by a single configuration flag:

| Property | Default | Description |
|----------|---------|-------------|
| `hoodie.datasource.read.use.dsv2` | `false` | When `true`, `HoodieCatalog.loadTable()` returns `HoodieInternalV2Table` directly (DSv2 read). When `false`, returns `V1Table` wrapper (current behavior). |

The flag already exists in `HoodieCatalog.loadTable()`. No new configuration is needed.

After all phases are complete and benchmarked, the default can be flipped to `true`.

## Test Plan

### Per-Phase Testing

Each phase includes targeted tests in `TestHoodieDSv2Read.java`:

| Phase | Test Coverage |
|-------|---------------|
| 1 | COW read, MOR read (base + log merge), partitioned/non-partitioned, column pruning, result parity with V1 |
| 2 | Partition filter pruning, data filter pushdown, unsupported filter fallback, combined filters |
| 3 | `sizeInBytes > 0` via `queryExecution.optimizedPlan.stats`, broadcast join selection for small tables |
| 4 | Schema evolution: add/rename column across commits, read all columns with DSv2 |
| 5 | Columnar batch output for COW (verify via `explain()`), result parity with row-based reads |

### Benchmark Suite

A consistent benchmark must be run before and after each phase to detect regressions:

1. **Scan throughput**: Full table scan (SELECT *) on COW and MOR tables with 10M+ rows
2. **Filter selectivity**: Point lookup, range scan, partition pruning on partitioned table
3. **Join performance**: Broadcast join (small table) and sort-merge join (large table) to
   validate CBO statistics impact
4. **Schema evolution overhead**: Read with evolved schema vs. stable schema

Results should be compared against the V1 baseline on the same dataset and hardware.

### Regression Protection

All DSv2 tests run with `hoodie.datasource.read.use.dsv2=true`. The existing V1 test suite
continues to run unchanged (default `false`), ensuring zero regression on the production path.
