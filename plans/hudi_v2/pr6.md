# PR6: Advanced Pushdowns + Mark PR5 Done

**Status: DONE**

## Context

PRs 1-5 of the DSv2 read implementation (RFC-98) are complete, delivering snapshot (CoW+MoR),
filter pushdown, incremental, and CDC query support. PR6 adds performance optimizations unique
to DSv2 that are not possible with the DSv1 read path: LIMIT pushdown, aggregate pushdown
(COUNT(*), MIN, MAX from column stats), and statistics reporting for CBO.

## Step 0: Mark PR5 as Done

Update `plans/hudi_v2/incremental_pr_plan.md`:
- PR 5 status: `**Status: DONE**` (change from NOT STARTED)
- Dependency graph: PR 5 line → `✅ DONE`
- PR 6 line: `⬜ NOT STARTED` → keep (will be updated when PR6 is done)

Update `plans/hudi_v2/pr5.md`:
- Status: `**Status: DONE**`

Create commit: `plans: PR 6 for advanced pushdowns, PR 5 is done`

---

## PR6 Scope

Three interfaces, prioritized by ROI:

1. **SupportsPushDownLimit** — `SELECT * FROM t LIMIT N` stops readers early
2. **SupportsReportStatistics** — Report size/row estimates to Spark CBO
3. **SupportsPushDownAggregates** — `COUNT(*)`, `MIN(col)`, `MAX(col)` from metadata table column stats

**Deferred to future PR:** `SupportsPushDownTopN` (requires in-memory sorting in readers; low ROI since Spark already handles ORDER BY efficiently post-scan).

---

## File Changes

### 1. `HoodieScanBuilder.scala` (modify)
`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieScanBuilder.scala`

Add interfaces:
```scala
class HoodieScanBuilder(...) extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns
  with SupportsPushDownLimit          // NEW
  with SupportsPushDownAggregates     // NEW
```

New imports:
```scala
import org.apache.spark.sql.connector.read.{SupportsPushDownLimit, SupportsPushDownAggregates}
import org.apache.spark.sql.connector.expressions.aggregate._
```

New fields:
```scala
private var pushedLimit: Option[Int] = None
private var pushedAggregation: Option[Aggregation] = None
private var aggregateResult: Option[Array[InternalRow]] = None
```

**SupportsPushDownLimit implementation:**
```scala
override def pushLimit(limit: Int): Boolean = {
  pushedLimit = Some(limit)
  true  // accepted (best-effort)
}
override def isPartiallyPushed(): Boolean = true  // each reader enforces locally
```

**SupportsPushDownAggregates implementation:**
```scala
override def pushAggregation(aggregation: Aggregation): Boolean = {
  // Only for snapshot queries, no GROUP BY, CoW tables (or fully compacted MoR)
  if (isCdcQuery || isIncrementalQuery || isReadOptimized) return false
  if (aggregation.groupByExpressions().nonEmpty) return false

  // Check column stats are available
  if (!MetadataPartitionType.COLUMN_STATS.isMetadataPartitionAvailable(metaClient)) return false

  // Validate all aggregate functions are supported: CountStar, Count, Min, Max
  val funcs = aggregation.aggregateExpressions()
  val allSupported = funcs.forall {
    case _: CountStar => true
    case c: Count => !c.isDistinct && c.columns.length == 1
    case _: Min => true
    case _: Max => true
    case _ => false
  }
  if (!allSupported) return false

  // Try to compute from column stats
  val result = tryComputeAggregates(aggregation)
  result match {
    case Some(rows) =>
      pushedAggregation = Some(aggregation)
      aggregateResult = Some(rows)
      true
    case None => false
  }
}

override def supportCompletePushDown(aggregation: Aggregation): Boolean = {
  pushedAggregation.contains(aggregation) && dataFilterExprs.isEmpty
}
```

**tryComputeAggregates helper:** Query `HoodieBackedTableMetadata.getColumnStats(...)` for files listed by `fileIndex`. Sum `valueCount` for COUNT(*), compute global min/max from per-file stats. Use `HoodieColumnRangeMetadata.merge()` pattern. For MoR with log files, return None (stats may be stale).

**Modify build():**
```scala
override def build(): Scan = {
  aggregateResult match {
    case Some(rows) =>
      // Build output schema from aggregation
      val outputSchema = buildAggregateOutputSchema(pushedAggregation.get)
      new HoodieLocalScan(outputSchema, rows)
    case None =>
      if (isCdcQuery) buildCdcScan()
      else if (isIncrementalQuery) buildIncrementalScan()
      else buildSnapshotScan()
  }
}
```

**Modify buildSnapshotScan() and buildIncrementalScan():** Pass `pushedLimit` to `HoodieBatchScan` constructor.

---

### 2. `HoodieBatchScan.scala` (modify)
`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieBatchScan.scala`

Add interface + parameter:
```scala
class HoodieBatchScan(...,
                      pushedLimit: Option[Int] = None)
  extends Scan with Batch with SupportsReportStatistics {
```

New import:
```scala
import org.apache.spark.sql.connector.read.{SupportsReportStatistics, Statistics}
import java.util.OptionalLong
```

**SupportsReportStatistics:**
```scala
override def estimateStatistics(): Statistics = {
  val totalSize = inputPartitions.collect {
    case p: HoodieInputPartition => p.baseFileLength
  }.sum
  new HoodieStatistics(totalSize)
}
```

**Pass limit to reader factory:**
```scala
override def createReaderFactory(): PartitionReaderFactory = {
  new HoodiePartitionReaderFactory(
    broadcastReader, broadcastConf, readSchema,
    requiredDataSchema, requiredPartitionSchema,
    morContext, includedCommitTimes,
    pushedLimit)  // NEW
}
```

**Update description():** Include limit info if pushed.

---

### 3. `HoodieCdcBatchScan.scala` (modify)
`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieCdcBatchScan.scala`

Add `SupportsReportStatistics` with basic size estimate from partition count.

---

### 4. `HoodiePartitionReaderFactory.scala` (modify)
`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodiePartitionReaderFactory.scala`

Add `pushedLimit: Option[Int] = None` parameter. Pass to both `HoodiePartitionReader` and `HoodieMorPartitionReader`.

---

### 5. `HoodiePartitionReader.scala` (modify)
`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodiePartitionReader.scala`

Add `pushedLimit: Option[Int] = None` parameter.

Add limit-aware iteration: wrap the final iterator with a counter that returns `false` from `hasNext` once limit is reached.

```scala
val limitedIter = pushedLimit match {
  case Some(limit) =>
    var count = 0
    projectedIter.takeWhile { _ =>
      count += 1
      count <= limit
    }
  case None => projectedIter
}
```

---

### 6. `HoodieMorPartitionReader.scala` (modify)
`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieMorPartitionReader.scala`

Same limit pattern as `HoodiePartitionReader`. Add `pushedLimit: Option[Int] = None` parameter. Wrap final iterator with limit-aware logic.

---

### 7. `HoodieLocalScan.scala` (new)
`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieLocalScan.scala`

```scala
class HoodieLocalScan(outputSchema: StructType, resultRows: Array[InternalRow])
  extends Scan with LocalScan {
  override def readSchema(): StructType = outputSchema
  override def rows(): Array[InternalRow] = resultRows
  override def description(): String = s"HoodieLocalScan[${resultRows.length} rows]"
}
```

---

### 8. `HoodieStatistics.scala` (new)
`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieStatistics.scala`

```scala
class HoodieStatistics(sizeBytes: Long, rowCount: Option[Long] = None)
  extends Statistics {
  override def sizeInBytes(): OptionalLong = OptionalLong.of(sizeBytes)
  override def numRows(): OptionalLong =
    rowCount.map(OptionalLong.of).getOrElse(OptionalLong.empty())
}
```

---

### 9. `TestDSv2Pushdowns.scala` (new test)
`hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/spark/sql/hudi/v2/TestDSv2Pushdowns.scala`

**Limit tests:**
- `testLimitReducesRowCount` — `SELECT * LIMIT 3` returns exactly 3 rows
- `testLimitGreaterThanTableSize` — LIMIT > table size returns all rows
- `testLimitWithFilter` — `SELECT * WHERE country = 'US' LIMIT 1`
- `testLimitOnMoRTable` — same tests with MoR
- `testLimitOnIncrementalQuery` — limit + incremental
- `testLimitDsv1VsDsv2` — results match between V1 and V2

**Statistics tests:**
- `testStatisticsReportsSizeInBytes` — verify non-zero sizeInBytes
- `testExplainIncludesStatistics` — EXPLAIN plan mentions statistics

**Aggregate pushdown tests (conditional on column stats availability):**
- `testCountStarPushdown` — verify correct count, plan shows `LocalTableScan`
- `testMinMaxPushdown` — `SELECT MIN(amount), MAX(amount)` from column stats
- `testCountWithPartitionFilter` — `COUNT(*) WHERE partition_col = 'US'`
- `testAggregateNotPushedWithGroupBy` — GROUP BY falls back to scan
- `testAggregateNotPushedForMoR` — MoR with logs falls back
- `testCountStarDsv1VsDsv2` — results match

---

## Key Design Decisions

1. **Limit is best-effort (isPartiallyPushed = true)**: Each reader enforces locally. Spark adds a final LocalLimit on top. This is the pattern Iceberg and Paimon use.

2. **Aggregate pushdown disabled for MoR with log files**: Column stats from base files don't account for updates/deletes in log files. Only fully compacted CoW tables get aggregate pushdown. This avoids correctness issues.

3. **LocalScan for aggregate results**: When aggregates are fully pushed, return a `HoodieLocalScan` (implements Spark's `LocalScan` interface) with pre-computed `InternalRow[]`. No file scanning at all.

4. **Statistics from input partitions**: `SupportsReportStatistics` sums `baseFileLength` from all `HoodieInputPartition` instances. Simple and always available.

5. **TopN deferred**: Requires in-memory sorting per reader, significant complexity for limited benefit. Spark handles ORDER BY efficiently post-scan.

---

## Reusable APIs

- `HoodieTableMetadata.getColumnStats(partitionNameFileNameList, columnName)` — get per-file column stats (`hudi-common`)
- `HoodieColumnRangeMetadata.merge(left, right)` — aggregate stats across files (`hudi-common/src/main/java/org/apache/hudi/stats/HoodieColumnRangeMetadata.java`)
- `MetadataPartitionType.COLUMN_STATS.isMetadataPartitionAvailable(metaClient)` — check column stats availability
- `HoodieFileIndex.filterFileSlices()` — already used for partition listing
- `Spark LocalScan` interface — `org.apache.spark.sql.connector.read.LocalScan` (confirmed in Spark 3.5.5 jar)

---

## Verification

```bash
# Build the module
mvn clean package -DskipTests -pl hudi-spark-datasource/hudi-spark-common -am

# Run the new test
mvn test -Pfunctional-tests \
  -pl hudi-spark-datasource/hudi-spark \
  -Dtest=skipJavaTests \
  -DwildcardSuites=org.apache.spark.sql.hudi.v2.TestDSv2Pushdowns

# Run all DSv2 tests to verify no regression
mvn test -Pfunctional-tests \
  -pl hudi-spark-datasource/hudi-spark \
  -Dtest=skipJavaTests \
  -DwildcardSuites=org.apache.spark.sql.hudi.v2

# Verify EXPLAIN plans show expected operators:
# - LIMIT query: BatchScanExec with limit info
# - COUNT(*) query: LocalTableScanExec (when pushed)
```
