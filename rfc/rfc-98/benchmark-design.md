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

# DSv2 Read Benchmark Design

This document describes the benchmark suite for validating DSv2 read performance
against the existing DSv1 baseline. The benchmarks are table-type-aware (COW and MOR)
and cover the four query patterns most relevant to the DSv2 read path.

## Design Principles

1. **Same benchmark code for V1 and V2.** The benchmark classes never set
   `hoodie.datasource.read.use.dsv2`. The read path is selected externally by
   setting (or not setting) the Spark config before launching the benchmark.
   This guarantees identical workloads and fair comparison.

2. **Reuse the existing framework.** All benchmarks extend `HoodieBenchmarkBase`
   and use `HoodieBenchmark` for timing, warm-up, and result formatting. This is
   the same framework used by `CowTableReadBenchmark` and other existing Hudi
   benchmarks.

3. **Deterministic data.** Row generation uses a fixed seed so that results are
   reproducible across runs. Partition assignment is derived from the record key
   to ensure even distribution.

4. **Minimal footprint.** Benchmarks run in `local[4]` mode with no external
   dependencies, so they can be executed on any developer workstation or CI node.

## Benchmark Matrix

| # | Case Name | Table Type | Query Pattern | Rows |
|---|-----------|------------|---------------|------|
| 1 | COW full scan | COW | `SELECT * FROM t` | 1 000 000 |
| 2 | COW column pruning | COW | `SELECT c1, c3 FROM t` | 1 000 000 |
| 3 | COW partition filter | COW | `SELECT * FROM t WHERE dt = '2024-01-05'` | 1 000 000 |
| 4 | COW aggregation | COW | `SELECT dt, SUM(c3) FROM t GROUP BY dt` | 1 000 000 |
| 5 | MOR full scan | MOR | `SELECT * FROM t` | 100 000 insert + 50 000 upsert |
| 6 | MOR column pruning | MOR | `SELECT c1, c3 FROM t` | 100 000 insert + 50 000 upsert |
| 7 | MOR partition filter | MOR | `SELECT * FROM t WHERE dt = '2024-01-05'` | 100 000 insert + 50 000 upsert |
| 8 | MOR aggregation | MOR | `SELECT dt, SUM(c3) FROM t GROUP BY dt` | 100 000 insert + 50 000 upsert |

### Table Setup

**COW tables** — 1 000 000 rows inserted in a single bulk-insert operation. The
table is partitioned by `dt` with 10 partitions (`2024-01-01` through `2024-01-10`),
giving ~100 000 rows per partition. This is large enough to measure scan throughput
while keeping setup time reasonable.

**MOR tables** — 100 000 rows inserted followed by a 50 000-row upsert that
updates existing records. The upsert produces delta log files alongside the base
files, exercising the log-merge code path in `HoodieFileGroupReader`. The table
uses the same 10 `dt` partitions.

### Schema

```
c1    INT          -- record key (unique, sequential)
c2    STRING       -- payload column
c3    DECIMAL(18,2) -- numeric column for aggregation
c4    TIMESTAMP    -- event time
dt    STRING       -- partition column (date string)
```

The schema is intentionally simple: it avoids complex/nested types that would
obscure read-path overhead. `c3` is a decimal to exercise non-trivial aggregation.

## Query Patterns

### 1. Full Scan (`SELECT *`)

Reads every column of every row. This is the worst-case workload for the read
path and establishes the throughput baseline.

**What it measures:** Raw scan speed, deserialization overhead, MOR log merge cost.

### 2. Column Pruning (`SELECT c1, c3`)

Reads only two of the five data columns. On the DSv2 path, `pruneColumns()` in
`HoodieScanBuilder` tells the reader to skip unrequested columns.

**What it measures:** Column pruning effectiveness. V2 should pass the required
schema through to the Parquet reader and avoid reading unused column data.

### 3. Partition Filter (`WHERE dt = '2024-01-05'`)

Selects a single partition (10% of data). On the DSv2 path, `pushFilters()` in
`HoodieScanBuilder` classifies this as a partition filter and passes it to
`HoodieFileIndex` for partition pruning.

**What it measures:** Partition pruning. Both V1 and V2 should prune to a single
partition, so the expected difference is planning overhead only.

### 4. Aggregation (`GROUP BY dt, SUM(c3)`)

A grouped aggregation that forces Spark to read `dt` and `c3`, then shuffle and
aggregate. This exercises column pruning implicitly (Spark only requests the
columns needed for the aggregation) and validates that DSv2 statistics
(`estimateStatistics()`) integrate correctly with Spark's CBO for exchange
planning.

**What it measures:** End-to-end query execution including Spark optimizer
interaction. Regression here may indicate issues with statistics or plan selection.

## Implementation

### Class Structure

```
hudi-spark-datasource/hudi-spark/src/test/scala/
  org/apache/spark/sql/execution/benchmark/
    DSv2ReadBenchmark.scala        -- COW + MOR benchmarks (8 cases)
```

A single benchmark object keeps all cases together so they share table setup
and run within one JVM, eliminating cross-process variance.

### Pseudocode

```scala
object DSv2ReadBenchmark extends HoodieBenchmarkBase {

  protected val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName(this.getClass.getCanonicalName)
    .withExtensions(new HoodieSparkSessionExtension)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    // NOTE: hoodie.datasource.read.use.dsv2 is NOT set here.
    // The caller controls V1 vs V2 via Spark config externally.
    .getOrCreate()

  private val cowRows = 1000000
  private val morInsertRows = 100000
  private val morUpsertRows = 50000
  private val numPartitions = 10
  private val seed = 42

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    cowBenchmarks()
    morBenchmarks()
  }

  private def cowBenchmarks(): Unit = withTempDir { dir =>
    val tableName = "cow_bench"
    val tablePath = new Path(dir.getCanonicalPath, tableName).toUri.toString
    prepareCowTable(tableName, tablePath)

    val benchmark = new HoodieBenchmark("DSv2 COW read", cowRows)

    benchmark.addCase("full scan") { _ =>
      spark.sql(s"SELECT * FROM $tableName").count()
    }
    benchmark.addCase("column pruning") { _ =>
      spark.sql(s"SELECT c1, c3 FROM $tableName").count()
    }
    benchmark.addCase("partition filter") { _ =>
      spark.sql(s"SELECT * FROM $tableName WHERE dt = '2024-01-05'").count()
    }
    benchmark.addCase("aggregation") { _ =>
      spark.sql(s"SELECT dt, SUM(c3) FROM $tableName GROUP BY dt").collect()
    }

    benchmark.run()
  }

  private def morBenchmarks(): Unit = withTempDir { dir =>
    val tableName = "mor_bench"
    val tablePath = new Path(dir.getCanonicalPath, tableName).toUri.toString
    prepareMorTable(tableName, tablePath)

    val benchmark = new HoodieBenchmark("DSv2 MOR read",
                                         morInsertRows + morUpsertRows)

    benchmark.addCase("full scan") { _ =>
      spark.sql(s"SELECT * FROM $tableName").count()
    }
    benchmark.addCase("column pruning") { _ =>
      spark.sql(s"SELECT c1, c3 FROM $tableName").count()
    }
    benchmark.addCase("partition filter") { _ =>
      spark.sql(s"SELECT * FROM $tableName WHERE dt = '2024-01-05'").count()
    }
    benchmark.addCase("aggregation") { _ =>
      spark.sql(s"SELECT dt, SUM(c3) FROM $tableName GROUP BY dt").collect()
    }

    benchmark.run()
  }

  private def prepareCowTable(name: String, path: String): Unit = {
    createDataFrame(cowRows).createOrReplaceTempView("cow_src")
    spark.sql(
      s"""CREATE TABLE $name USING hudi
         |TBLPROPERTIES (
         |  primaryKey = 'c1',
         |  type = 'cow'
         |)
         |PARTITIONED BY (dt)
         |LOCATION '$path'
         |AS SELECT * FROM cow_src""".stripMargin)
  }

  private def prepareMorTable(name: String, path: String): Unit = {
    // Initial insert
    createDataFrame(morInsertRows).createOrReplaceTempView("mor_src")
    spark.sql(
      s"""CREATE TABLE $name USING hudi
         |TBLPROPERTIES (
         |  primaryKey = 'c1',
         |  type = 'mor'
         |)
         |PARTITIONED BY (dt)
         |LOCATION '$path'
         |AS SELECT * FROM mor_src""".stripMargin)

    // Upsert to create log files
    createUpsertDataFrame(morUpsertRows).createOrReplaceTempView("mor_upsert")
    spark.sql(
      s"""INSERT INTO $name
         |SELECT * FROM mor_upsert""".stripMargin)
  }

  // Creates DataFrame with sequential keys distributed across 10 partitions
  private def createDataFrame(numRows: Int): DataFrame = { ... }

  // Creates DataFrame that overlaps with existing keys to trigger upsert
  private def createUpsertDataFrame(numRows: Int): DataFrame = { ... }
}
```

### Running the Benchmark

```bash
# V1 baseline (default — DSv2 disabled)
spark-submit --class org.apache.spark.sql.execution.benchmark.DSv2ReadBenchmark \
  hudi-spark-bundle.jar

# V2 candidate
spark-submit --class org.apache.spark.sql.execution.benchmark.DSv2ReadBenchmark \
  --conf hoodie.datasource.read.use.dsv2=true \
  hudi-spark-bundle.jar
```

Or via Maven (within the `hudi-spark` module):

```bash
# V1 baseline
mvn test -pl hudi-spark-datasource/hudi-spark \
  -Dtest=skipJavaTests \
  -DwildcardSuites=org.apache.spark.sql.execution.benchmark.DSv2ReadBenchmark

# V2 candidate — set Spark config via system property
mvn test -pl hudi-spark-datasource/hudi-spark \
  -Dtest=skipJavaTests \
  -DwildcardSuites=org.apache.spark.sql.execution.benchmark.DSv2ReadBenchmark \
  -Dspark.hoodie.datasource.read.use.dsv2=true
```

Alternatively, set `SPARK_GENERATE_BENCHMARK_FILES=1` to persist results to
`benchmarks/DSv2ReadBenchmark-jdk<version>-results.txt` for archival comparison.

## Expected Output Format

The `HoodieBenchmark` framework produces a formatted table for each suite:

```
Java HotSpot(TM) 64-Bit Server VM 11.0.x on Linux 5.x
Intel(R) Core(TM) i7-XXXX CPU @ X.XXGHz
DSv2 COW read:                    Best Time(ms)   Avg Time(ms)   Stdev(ms)    Rate(M/s)   Per Row(ns)   Relative
-----------------------------------------------------------------------------------------------------------------
full scan                                  1200           1250          40          0.8        1200.0       1.0X
column pruning                              600            630          25          1.6         600.0       2.0X
partition filter                            150            165          12          6.7         150.0       8.0X
aggregation                                 700            720          30          1.4         700.0       1.7X
```

(Times above are illustrative, not actual measurements.)

## Success Criteria

A benchmark run **passes** when:

1. All 8 cases complete without error on both V1 and V2 paths.
2. V2 results are **within 10%** of V1 on full scan and column pruning cases.
   Small regressions in early phases (row-based MOR reads) are acceptable and
   documented.
3. Partition filter cases show equivalent pruning effectiveness (same row count,
   comparable latency).
4. No benchmark case shows a **>20% regression** compared to V1. If one does,
   the phase is not ready to merge and the regression must be investigated.

These thresholds are guidelines for development; the numbers in the result files
are the authoritative record for each phase.

## Per-Phase Benchmark Expectations

| Phase | Expected Impact |
|-------|-----------------|
| 1 (End-to-end read) | V2 may be slightly slower due to row-based reads and new planning path. Baseline establishment. |
| 2 (Filter pushdown) | Partition filter case should show comparable or better pruning. Data filter pushdown may reduce full-scan times. |
| 3 (Statistics/CBO) | Aggregation case may benefit from better join/exchange planning. Minimal impact on scan cases. |
| 4 (Schema evolution) | No regression expected on stable-schema benchmarks. |
| 5 (Columnar reads) | COW cases should match or exceed V1 performance due to vectorized Parquet reading. MOR cases unchanged. |
