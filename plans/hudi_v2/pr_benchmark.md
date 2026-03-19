# Plan: Update Benchmark to Cover All DSv2 Read Features

## Context

The DSv2 read implementation (RFC-98) added several read features across 9 commits:
- COW/MOR snapshot read, filter pushdown, limit pushdown, aggregate pushdown, incremental read, CDC read

The current benchmark (`benchmarks/hudi_benchmark.scala`) only tests full scan and projected read for DSv1/DSv2.
It needs to be extended to benchmark ALL DSv2 read features and include a performance comparison showing
where DSv2 is faster than DSv1.

## Files to Modify

- `benchmarks/hudi_benchmark.scala` - main benchmark script
- `benchmarks/README.md` - documentation

## Changes

### 1. Configuration (Section 1)

- Change `HUDI_BENCHMARK_DSV2_ENABLED` default from `"false"` to `"true"` (line 20)
- Add new env vars:
  - `HUDI_BENCHMARK_LIMIT_VALUE` (default: `"1000"`) - LIMIT value for limit pushdown benchmark
  - `HUDI_BENCHMARK_FILTER_COL` (default: `""`, auto-detected) - column for filter benchmark

### 2. Helper Functions (Section 3)

Add after existing helpers (line 88):

- `getTablePath(tableName: String): String` - resolves managed table HDFS path via `spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName, Some(benchmarkDb))).location.toString`
- `getLastCompletionTime(tablePath: String): String` - gets the latest commit completion time from `HoodieTableMetaClient`'s active timeline. Uses:
  ```scala
  import org.apache.hudi.common.table.HoodieTableMetaClient
  import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
  ```

### 3. Auto-Detection (Section 4, after line 134)

After schema summary, auto-detect:
- `filterCol` - column for filter benchmark. If `HUDI_BENCHMARK_FILTER_COL` set, use it. Else use first projected column.
- `filterValue` - midpoint value of `filterCol` from source_data: `SELECT percentile_approx(CAST($filterCol AS DOUBLE), 0.5) FROM source_data`. If fails (non-numeric), fall back to `IS NOT NULL` filter.
- `filterExpr` - final WHERE expression, e.g., `"$filterCol >= $filterValue"` or `"$filterCol IS NOT NULL"`

### 4. New Time Collectors (Section 5, after line 157)

```scala
var readFilterCowDsv1Times: Seq[Double] = Seq.empty
var readFilterMorDsv1Times: Seq[Double] = Seq.empty
var readFilterCowDsv2Times: Seq[Double] = Seq.empty
var readFilterMorDsv2Times: Seq[Double] = Seq.empty
var readLimitCowDsv1Times: Seq[Double] = Seq.empty
var readLimitMorDsv1Times: Seq[Double] = Seq.empty
var readLimitCowDsv2Times: Seq[Double] = Seq.empty
var readLimitMorDsv2Times: Seq[Double] = Seq.empty
var aggCountDsv1Times: Seq[Double] = Seq.empty
var aggCountDsv2Times: Seq[Double] = Seq.empty
var aggMinMaxDsv1Times: Seq[Double] = Seq.empty
var aggMinMaxDsv2Times: Seq[Double] = Seq.empty
var incrReadDsv1Times: Seq[Double] = Seq.empty
var incrReadDsv2Times: Seq[Double] = Seq.empty
var cdcReadDsv1Times: Seq[Double] = Seq.empty
var cdcReadDsv2Times: Seq[Double] = Seq.empty
```

### 5. New Benchmark Sections

All new sections go after existing clustering benchmark (5f) and before final summary (Section 6).
Each follows the pattern: warmup, timed iterations, per-section summary.
New sections only run when `dsv2Enabled` is true.

#### 5g: Filter Pushdown Read

- Reuse `cow_bulk_insert_1` and `mor_bulk_insert_1` tables
- Query: `SELECT $projectedCols FROM $table WHERE $filterExpr`
- Sink to noop
- Toggle `hoodie.datasource.read.use.v2` for DSv1/DSv2
- 4 combos: COW x DSv1/DSv2, MOR x DSv1/DSv2
- Warmup: LIMIT 100000 with filter for both COW/MOR

#### 5h: Limit Pushdown Read

- Reuse `cow_bulk_insert_1` and `mor_bulk_insert_1` tables
- Query: `SELECT * FROM $table LIMIT $limitValue`
- Sink to noop
- Toggle `hoodie.datasource.read.use.v2` for DSv1/DSv2
- 4 combos: COW x DSv1/DSv2, MOR x DSv1/DSv2
- **This is the primary scenario where DSv2 should be faster** (DSv2 stops reading partitions after limit rows, DSv1 reads all data)
- Warmup: LIMIT 100 for both COW/MOR

#### 5i: Aggregate Pushdown

- Create dedicated COW table `cow_agg` with column stats metadata enabled:
  ```sql
  CREATE TABLE cow_agg USING hudi TBLPROPERTIES (
    type = 'cow', primaryKey = '...', preCombineField = '...',
    'hoodie.metadata.index.column.stats.enable' = 'true',
    'hoodie.bulkinsert.shuffle.parallelism' = '200',
    'hoodie.compact.inline' = 'false', 'hoodie.clustering.inline' = 'false'
  ) AS SELECT * FROM source_data
  ```
- Track in `createdTables`
- Queries:
  - COUNT(*): `SELECT COUNT(*) FROM cow_agg` (sink to noop)
  - MIN/MAX: `SELECT MIN($filterCol), MAX($filterCol) FROM cow_agg` (skip if no numeric column detected)
- Toggle `hoodie.datasource.read.use.v2` for DSv1/DSv2
- 2 combos per query: DSv1, DSv2
- DSv2 resolves from column stats metadata (near-instant), DSv1 scans all files

#### 5j: Incremental Read

- Create dedicated COW table `cow_incr` with bulk_insert of all source_data
- Capture commit1 time via `getLastCompletionTime(getTablePath("cow_incr"))`
- INSERT subset: `INSERT INTO cow_incr SELECT * FROM source_data LIMIT 500000`
- Capture commit2 time
- Track in `createdTables`
- Use DataFrame API for reads (incremental queries need per-reader options):
  - DSv1: `spark.read.format("hudi").option("hoodie.datasource.query.type", "incremental").option("hoodie.datasource.read.begin.instanttime", commit1Time).load(tablePath)`
  - DSv2: `spark.read.format("hudi_v2").option(same).load(tablePath)`
- Sink to noop
- 2 combos: DSv1, DSv2

#### 5k: CDC Read

- Create dedicated COW table `cow_cdc` with CDC enabled:
  ```sql
  CREATE TABLE cow_cdc USING hudi TBLPROPERTIES (
    type = 'cow', primaryKey = '...', preCombineField = '...',
    'hoodie.table.cdc.enabled' = 'true',
    'hoodie.table.cdc.supplemental.logging.mode' = 'DATA_BEFORE_AFTER',
    'hoodie.bulkinsert.shuffle.parallelism' = '200',
    'hoodie.compact.inline' = 'false', 'hoodie.clustering.inline' = 'false'
  ) AS SELECT * FROM source_data
  ```
- Capture commit1 time
- INSERT subset: `INSERT INTO cow_cdc SELECT * FROM source_data LIMIT 500000`
- Track in `createdTables`
- Use DataFrame API for reads:
  - DSv1: `spark.read.format("hudi").option("hoodie.datasource.query.type", "incremental").option("hoodie.datasource.query.incremental.format", "cdc").option("hoodie.datasource.read.begin.instanttime", commit1Time).load(tablePath)`
  - DSv2: `spark.read.format("hudi_v2").option(same).load(tablePath)`
- Sink to noop
- 2 combos: DSv1, DSv2

### 6. Final Summary (Section 6) - Extended

Add new benchmark stats after existing ones:
- Filter read stats (4 lines: COW/MOR x DSv1/DSv2)
- Limit read stats (4 lines)
- Aggregate COUNT(*) stats (2 lines: DSv1/DSv2)
- Aggregate MIN/MAX stats (2 lines, if applicable)
- Incremental read stats (2 lines)
- CDC read stats (2 lines)

All conditional on `dsv2Enabled`.

### 7. Performance Comparison Section (new, after final summary)

When `dsv2Enabled`, add a "DSv2 vs DSv1 PERFORMANCE COMPARISON" section:

- Helper function `comparePerf(label, dsv1Times, dsv2Times): Boolean` that:
  - Computes DSv1 avg, DSv2 avg, speedup = dsv1Avg / dsv2Avg
  - Prints: `label: DSv1 avg Xs, DSv2 avg Ys, speedup Zx (DSv2 FASTER / DSv1 FASTER)`
  - Returns `true` if DSv2 is faster

- Compare all 12 read scenario pairs:
  - Full scan COW, Full scan MOR
  - Projected COW, Projected MOR
  - Filter COW, Filter MOR
  - Limit COW, Limit MOR
  - Aggregate COUNT(*), Aggregate MIN/MAX
  - Incremental, CDC

- Final check:
  - Count scenarios where DSv2 is faster
  - Print `PASS: DSv2 is faster than DSv1 in N of M scenarios`
  - Or `WARNING: DSv2 was not faster than DSv1 in any scenario`

### 8. README.md Updates

- Update `Benchmark Scenarios` table with 5 new rows (filter, limit, aggregate, incremental, CDC)
- Update `Environment Variables` table: change DSV2_ENABLED default to `true`, add LIMIT_VALUE and FILTER_COL
- Add `Scenarios Configuration` subsections for each new scenario
- Update `Expected Output` to include new lines and comparison section
- Remove "POC stub" comment from DSV2_ENABLED description

### Option Key Reference (for raw strings in benchmark)

| Config | Key string |
|--------|-----------|
| Query type | `hoodie.datasource.query.type` |
| Query type values | `snapshot`, `incremental`, `read_optimized` |
| Incremental format | `hoodie.datasource.query.incremental.format` |
| Incremental format values | `latest_state`, `cdc` |
| Start commit | `hoodie.datasource.read.begin.instanttime` |
| End commit | `hoodie.datasource.read.end.instanttime` |
| DSv2 toggle | `hoodie.datasource.read.use.v2` |
| Column stats enable | `hoodie.metadata.index.column.stats.enable` |
| CDC enable | `hoodie.table.cdc.enabled` |
| CDC logging mode | `hoodie.table.cdc.supplemental.logging.mode` |

## Verification

1. Build the Hudi Spark bundle: `mvn clean package -DskipTests -Dspark3.5 -pl packaging/hudi-spark-bundle -am`
2. Run benchmark with small data (`part-0000*.parquet` glob, ~1.25 GB):
   ```bash
   export HUDI_BENCHMARK_DSV2_ENABLED=true
   export HUDI_BENCHMARK_ITERATIONS=1
   ```
3. Verify all new sections execute without errors
4. Verify the performance comparison section prints speedup ratios
5. Verify limit pushdown shows DSv2 faster than DSv1 (strongest expected advantage)
6. Check log file contains complete output
