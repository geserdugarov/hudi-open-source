# Hudi Benchmark

## Overview

Macro-level benchmark comparing Hudi **COW vs MOR** table types across write, read, compaction,
and clustering operations. Also supports comparing **DSv1 vs DSv2** read paths including
filter pushdown, limit pushdown, aggregate pushdown, incremental read, and CDC read.
Targets real-world Parquet data on HDFS.
Currently, the benchmark expects run on local machine.

## Test Data

| Property      | Value               |
|---------------|---------------------|
| Source format | Parquet             |
| Files         | ~800                |
| Columns       | 300 (all primitive) |
| Total size    | ~100GB              |
| Storage       | HDFS                |

The source path supports globs for quick smoke tests:
- `part-0000*.parquet` — ~10 files, ~1.25 GB (small)
- `part-000*.parquet` — ~100 files, ~12.5 GB (medium)
- `part-00*.parquet` — ~800 files, ~100 GB (full)

## Benchmark Scenarios

| Scenario             | Modes                                             | Description                                              |
|----------------------|---------------------------------------------------|----------------------------------------------------------|
| Write (bulk_insert)  | COW, MOR                                          | CTAS with `bulk_insert` operation, no indexing            |
| Write (insert)       | COW, MOR                                          | CTAS with `insert` operation, with indexing               |
| Read (full scan)     | COW×DSv1, COW×DSv2, MOR×DSv1, MOR×DSv2           | `SELECT * FROM table` via noop sink                       |
| Read (projected)     | COW×DSv1, COW×DSv2, MOR×DSv1, MOR×DSv2           | `SELECT col_subset FROM table` via noop sink              |
| Read (filter)        | COW×DSv1, COW×DSv2, MOR×DSv1, MOR×DSv2           | `SELECT cols FROM table WHERE expr` via noop sink         |
| Read (limit)         | COW×DSv1, COW×DSv2, MOR×DSv1, MOR×DSv2           | `SELECT * FROM table LIMIT N` via noop sink               |
| Aggregate pushdown   | DSv1, DSv2                                        | `COUNT(*)` and `MIN/MAX` on COW with column stats         |
| Incremental read     | DSv1, DSv2                                        | Incremental query between two commits via DataFrame API   |
| CDC read             | DSv1, DSv2                                        | CDC query between two commits via DataFrame API           |
| Compaction           | MOR only                                          | Schedule + run compaction after upserts create logs       |
| Clustering           | COW only                                          | Run clustering after writes create small files            |

## Environment Variables

| Variable                         | Required | Default | Description                                                    |
|----------------------------------|----------|---------|----------------------------------------------------------------|
| `HUDI_BENCHMARK_DATA_PATH`      | Yes      | —       | HDFS path to source Parquet files (supports globs)             |
| `HUDI_BENCHMARK_RECORD_KEY`     | Yes      | —       | Primary key column name                                        |
| `HUDI_BENCHMARK_PRECOMBINE_FIELD`| Yes     | —       | Ordering/precombine field name                                 |
| `HUDI_BENCHMARK_PARTITION_FIELD`| No       | (empty) | Partition field; empty = non-partitioned                       |
| `HUDI_BENCHMARK_ITERATIONS`     | No       | `1`     | Timed iterations per benchmark case                            |
| `HUDI_BENCHMARK_PROJECTED_COLS` | No       | (auto)  | Comma-separated column subset for projected reads; auto-detects first 6 non-key columns if unset |
| `HUDI_BENCHMARK_DSV2_ENABLED`   | No       | `true`  | Include DSv2 read benchmarks (filter, limit, aggregate, incremental, CDC) |
| `HUDI_BENCHMARK_LIMIT_VALUE`    | No       | `1000`  | LIMIT value for limit pushdown benchmark                       |
| `HUDI_BENCHMARK_FILTER_COL`     | No       | (auto)  | Column for filter benchmark; auto-detects first projected column if unset |

## Prerequisites

### Build the Hudi Spark bundle

```bash
mvn clean package -DskipTests -Dspark3.5 -pl packaging/hudi-spark-bundle -am
```

The resulting JAR is at `packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-*.jar`.

## Running the Benchmark

### Environment setup

Create a `.env` file or export variables:

```bash
export HUDI_BENCHMARK_DATA_PATH="hdfs:///data/benchmark/part-000*.parquet"
export HUDI_BENCHMARK_RECORD_KEY="uuid_key"
export HUDI_BENCHMARK_PRECOMBINE_FIELD="primary_key"
# Optional:
export HUDI_BENCHMARK_PARTITION_FIELD=""
export HUDI_BENCHMARK_ITERATIONS=1
export HUDI_BENCHMARK_PROJECTED_COLS=""
export HUDI_BENCHMARK_DSV2_ENABLED=true
export HUDI_BENCHMARK_LIMIT_VALUE=1000
export HUDI_BENCHMARK_FILTER_COL=""
```

### Via spark-shell

```bash
source ./benchmarks/.env && \
$SPARK_HOME/bin/spark-shell \
    --master $SPARK_CLUSTER \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 6g \
    --total-executor-cores 9 \
    --jars $HUDI_JAR,$HIVE_JARS \
    --conf spark.local.dir=$SPARK_LOCAL_DIR \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar \
    --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hadoop \
    --conf spark.sql.catalogImplementation=in-memory \
    -i ./benchmarks/hudi_benchmark.scala
```

Where `$HUDI_JAR` points to the Hudi Spark bundle JAR.

## Scenarios Configuration

### Write (bulk_insert)
Pure write throughput without indexing overhead:
```
hoodie.datasource.write.operation = bulk_insert
hoodie.bulkinsert.shuffle.parallelism = 200
hoodie.compact.inline = false
hoodie.clustering.inline = false
```

### Write (insert)
Write with bloom filter indexing:
```
hoodie.datasource.write.operation = insert
hoodie.compact.inline = false
hoodie.clustering.inline = false
```

### Read (DSv1)
Standard Hudi read path:
```
hoodie.datasource.read.use.v2 = false
```

### Read (DSv2)
V2 read path with pushdown support:
```
hoodie.datasource.read.use.v2 = true
```

### Read (filter pushdown)
Filter expression is auto-detected from the first projected column. For numeric columns, uses
`col >= median_value`. For non-numeric columns, falls back to `col IS NOT NULL`. Override with
`HUDI_BENCHMARK_FILTER_COL`.

### Read (limit pushdown)
Uses `SELECT * FROM table LIMIT N` where N defaults to 1000. DSv2 stops reading after collecting
enough rows, while DSv1 reads all data first. This is the primary scenario where DSv2 should be
significantly faster.

### Aggregate pushdown
Creates a dedicated COW table with `hoodie.metadata.index.column.stats.enable = true`.
Benchmarks `COUNT(*)` and `MIN/MAX` (if numeric column detected). DSv2 resolves aggregates from
column stats metadata (near-instant), while DSv1 scans all files.

### Incremental read
Creates a COW table, captures commit time, inserts a subset, then reads the incremental delta
between the two commits using DataFrame API with `hoodie.datasource.query.type = incremental`.

### CDC read
Creates a COW table with `hoodie.table.cdc.enabled = true` and
`hoodie.table.cdc.supplemental.logging.mode = DATA_BEFORE_AFTER`. Inserts a subset, then reads
CDC changes using `hoodie.datasource.query.incremental.format = cdc`.

### Compaction (MOR)
Schedule and execute compaction after upserts create log files:
```
hoodie.compact.inline = false
hoodie.parquet.small.file.limit = 0
hoodie.parquet.max.file.size = 33554432
```

### Clustering (COW)
Recluster small files into larger ones:
```
hoodie.clustering.inline = false
hoodie.parquet.small.file.limit = 0
hoodie.parquet.max.file.size = 33554432
```

## Log Output

The benchmark script duplicates all stdout to a timestamped log file
(`hudi_benchmark_YYYYMMDD_HHmmss.log`) in the current working directory.
This preserves the full output for later analysis even if the terminal buffer is lost.

## Error Handling

The benchmark wraps all timed iterations in a `try`/`finally` block.
On completion (or failure), the `finally` block drops all benchmark tables created during the run,
drops the benchmark database, unpersists cached source data, and closes the log file.

## Expected Output

```
============================================================
BENCHMARK COMPLETE
============================================================

Write bulk_insert (COW)      : 12.3s  (min: 12.3s, max: 12.3s, avg: 12.3s)
Write bulk_insert (MOR)      : 10.5s  (min: 10.5s, max: 10.5s, avg: 10.5s)
Write insert (COW)           : 25.1s  (min: 25.1s, max: 25.1s, avg: 25.1s)
Write insert (MOR)           : 22.3s  (min: 22.3s, max: 22.3s, avg: 22.3s)
Read full (COW, DSv1)        : 8.2s  (min: 8.2s, max: 8.2s, avg: 8.2s)
Read full (MOR, DSv1)        : 9.1s  (min: 9.1s, max: 9.1s, avg: 9.1s)
Read full (COW, DSv2)        : 7.8s  (min: 7.8s, max: 7.8s, avg: 7.8s)
Read full (MOR, DSv2)        : 8.5s  (min: 8.5s, max: 8.5s, avg: 8.5s)
Read projected (COW, DSv1)   : 3.4s  (min: 3.4s, max: 3.4s, avg: 3.4s)
Read projected (MOR, DSv1)   : 3.8s  (min: 3.8s, max: 3.8s, avg: 3.8s)
Read projected (COW, DSv2)   : 3.2s  (min: 3.2s, max: 3.2s, avg: 3.2s)
Read projected (MOR, DSv2)   : 3.5s  (min: 3.5s, max: 3.5s, avg: 3.5s)
Compaction (MOR)             : 45.6s  (min: 45.6s, max: 45.6s, avg: 45.6s)
Clustering (COW)             : 38.2s  (min: 38.2s, max: 38.2s, avg: 38.2s)
Read filter (COW, DSv1)      : 4.1s  (min: 4.1s, max: 4.1s, avg: 4.1s)
Read filter (MOR, DSv1)      : 4.5s  (min: 4.5s, max: 4.5s, avg: 4.5s)
Read filter (COW, DSv2)      : 3.8s  (min: 3.8s, max: 3.8s, avg: 3.8s)
Read filter (MOR, DSv2)      : 4.2s  (min: 4.2s, max: 4.2s, avg: 4.2s)
Read limit (COW, DSv1)       : 5.0s  (min: 5.0s, max: 5.0s, avg: 5.0s)
Read limit (MOR, DSv1)       : 5.5s  (min: 5.5s, max: 5.5s, avg: 5.5s)
Read limit (COW, DSv2)       : 0.3s  (min: 0.3s, max: 0.3s, avg: 0.3s)
Read limit (MOR, DSv2)       : 0.4s  (min: 0.4s, max: 0.4s, avg: 0.4s)
Aggregate COUNT(*) (DSv1)    : 6.2s  (min: 6.2s, max: 6.2s, avg: 6.2s)
Aggregate COUNT(*) (DSv2)    : 0.1s  (min: 0.1s, max: 0.1s, avg: 0.1s)
Aggregate MIN/MAX (DSv1)     : 6.5s  (min: 6.5s, max: 6.5s, avg: 6.5s)
Aggregate MIN/MAX (DSv2)     : 0.1s  (min: 0.1s, max: 0.1s, avg: 0.1s)
Incremental read (DSv1)      : 2.1s  (min: 2.1s, max: 2.1s, avg: 2.1s)
Incremental read (DSv2)      : 1.8s  (min: 1.8s, max: 1.8s, avg: 1.8s)
CDC read (DSv1)              : 3.2s  (min: 3.2s, max: 3.2s, avg: 3.2s)
CDC read (DSv2)              : 2.9s  (min: 2.9s, max: 2.9s, avg: 2.9s)

============================================================
DSv2 vs DSv1 PERFORMANCE COMPARISON
============================================================

Full scan (COW)                    : DSv1 avg 8.2s, DSv2 avg 7.8s, speedup 1.05x (DSv2 FASTER)
Full scan (MOR)                    : DSv1 avg 9.1s, DSv2 avg 8.5s, speedup 1.07x (DSv2 FASTER)
Projected (COW)                    : DSv1 avg 3.4s, DSv2 avg 3.2s, speedup 1.06x (DSv2 FASTER)
Projected (MOR)                    : DSv1 avg 3.8s, DSv2 avg 3.5s, speedup 1.09x (DSv2 FASTER)
Filter (COW)                       : DSv1 avg 4.1s, DSv2 avg 3.8s, speedup 1.08x (DSv2 FASTER)
Filter (MOR)                       : DSv1 avg 4.5s, DSv2 avg 4.2s, speedup 1.07x (DSv2 FASTER)
Limit (COW)                        : DSv1 avg 5.0s, DSv2 avg 0.3s, speedup 16.67x (DSv2 FASTER)
Limit (MOR)                        : DSv1 avg 5.5s, DSv2 avg 0.4s, speedup 13.75x (DSv2 FASTER)
Aggregate COUNT(*)                 : DSv1 avg 6.2s, DSv2 avg 0.1s, speedup 62.00x (DSv2 FASTER)
Aggregate MIN/MAX                  : DSv1 avg 6.5s, DSv2 avg 0.1s, speedup 65.00x (DSv2 FASTER)
Incremental                        : DSv1 avg 2.1s, DSv2 avg 1.8s, speedup 1.17x (DSv2 FASTER)
CDC                                : DSv1 avg 3.2s, DSv2 avg 2.9s, speedup 1.10x (DSv2 FASTER)

PASS: DSv2 is faster than DSv1 in 12 of 12 scenarios
```

(Numbers are illustrative — actual results depend on hardware and cluster config.)
