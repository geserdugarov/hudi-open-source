# Spark DataSource V2 Read Path

## Introduction

The V2 read path is the **actual** read implementation for all Paimon tables in Spark. Regardless of whether a table was written via V1 or V2, all reads go through the DataSource V2 API. `PaimonSparkTableBase` always advertises `BATCH_READ` and `MICRO_BATCH_READ` capabilities.

This document covers the full read pipeline: scan building with predicate/projection push-down, split planning, partition reader creation, streaming reads, and runtime filtering.

## Table Capabilities

`PaimonSparkTableBase.capabilities()` always includes:

```scala
// PaimonSparkTableBase.scala:92-109
val capabilities = JEnumSet.of(
  TableCapability.BATCH_READ,
  TableCapability.OVERWRITE_BY_FILTER,
  TableCapability.MICRO_BATCH_READ
)
```

## Entry Point: `newScanBuilder()`

`PaimonSparkTableBase.newScanBuilder()` dispatches to the appropriate scan builder:

```scala
// PaimonSparkTableBase.scala:132-141
override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
  table match {
    case t: KnownSplitsTable =>
      new PaimonSplitScanBuilder(t)
    case _: InnerTable =>
      new PaimonScanBuilder(table.copy(options.asCaseSensitiveMap).asInstanceOf[InnerTable])
    case _ =>
      throw new RuntimeException("Only InnerTable can be scanned.")
  }
}
```

- `KnownSplitsTable`: Uses `PaimonSplitScanBuilder` for tables with pre-determined splits
- `InnerTable` (most tables): Uses `PaimonScanBuilder` with user-provided scan options applied

## ScanBuilder: Push-Down Optimizations

### `PaimonBaseScanBuilder`

The base scan builder (`org.apache.paimon.spark.PaimonBaseScanBuilder`) implements three push-down interfaces:

#### Column Pruning: `SupportsPushDownRequiredColumns`

```scala
override def pruneColumns(requiredSchema: StructType): Unit = {
  this.requiredSchema = requiredSchema
}
```

Spark pushes down only the columns referenced in the query, reducing I/O.

#### Filter Push-Down: `SupportsPushDownV2Filters`

```scala
// PaimonBaseScanBuilder.scala:63-110
override def pushPredicates(predicates: Array[SparkPredicate]): Array[SparkPredicate]
```

The filter push-down process:
1. Each Spark predicate is converted to a Paimon `Predicate` via `SparkV2FilterConverter`
2. Partition-only predicates are separated using `PartitionPredicateVisitor`
3. Supported filter types: `=`, `<>`, `<`, `<=`, `>`, `>=`, `IN`, `IS NULL`, `IS NOT NULL`, `AND`, `OR`, `NOT`, `STARTS_WITH`, `ENDS_WITH`, `CONTAINS`
4. Partition filters -> `pushedPartitionFilters` (used for manifest-level pruning)
5. Data filters -> `pushedDataFilters` (used for data-level filtering)
6. Unsupported filters are returned as post-scan predicates

#### Limit Push-Down: `SupportsPushDownLimit`

```scala
// PaimonBaseScanBuilder.scala:116-121
override def pushLimit(limit: Int): Boolean = {
  pushedLimit = Some(limit)
  false  // best-effort, returns false to indicate limit is not fully pushed
}
```

### `PaimonScanBuilder`

`PaimonScanBuilder` (`org.apache.paimon.spark.PaimonScanBuilder`) extends `PaimonBaseScanBuilder` with additional capabilities:

#### Aggregation Push-Down: `SupportsPushDownAggregates`

```scala
// PaimonScanBuilder.scala:97-125
override def pushAggregation(aggregation: Aggregation): Boolean
```

When aggregations can be computed entirely from metadata (e.g., `COUNT(*)`, `MIN`/`MAX` on sorted columns), Paimon produces a `PaimonLocalScan` that returns pre-computed results without reading any data files. Requirements:
- The table must be a `FileStoreTable`
- No post-scan predicates
- The aggregation must be fully supported by `AggregatePushDownUtils.tryPushdownAggregation()`

#### TopN Push-Down: `SupportsPushDownTopN`

```scala
// PaimonScanBuilder.scala:42-87
override def pushTopN(orders: Array[SortOrder], limit: Int): Boolean
```

Pushes `ORDER BY ... LIMIT N` patterns down to the scan layer, enabling Paimon to prune splits that cannot contribute to the top-N result.

#### Build

```scala
// PaimonScanBuilder.scala:127-152
override def build(): Scan = {
  localScan match {
    case Some(scan) => scan  // aggregation push-down produced a local scan
    case None =>
      PaimonScan(actualTable, requiredSchema, pushedPartitionFilters,
        pushedDataFilters, pushedLimit, pushedTopN, vectorSearch)
  }
}
```

## `PaimonScan`: Scan Planning

`PaimonScan` (`org.apache.paimon.spark.PaimonScan`) extends `PaimonBaseScan` (the `BaseScan` trait) and adds bucketed scan support.

### `BaseScan`: Core Scan Logic

`BaseScan` (`org.apache.paimon.spark.read.BaseScan`) constructs a `ReadBuilder` with all pushed-down predicates and projections:

```scala
// BaseScan.scala:104-116
lazy val readBuilder: ReadBuilder = {
  val _readBuilder = table.newReadBuilder().withReadType(readTableRowType)
  if (pushedPartitionFilters.nonEmpty) {
    _readBuilder.withPartitionFilter(PartitionPredicate.and(pushedPartitionFilters.asJava))
  }
  if (pushedDataFilters.nonEmpty) {
    _readBuilder.withFilter(pushedDataFilters.asJava)
  }
  pushedLimit.foreach(_readBuilder.withLimit)
  pushedTopN.foreach(_readBuilder.withTopN)
  pushedVectorSearch.foreach(_readBuilder.withVectorSearch)
  _readBuilder.dropStats()
}
```

#### Metadata Column Support

`BaseScan` separates the required schema into table fields and metadata fields:

```scala
// BaseScan.scala:77-84
private[paimon] val (readTableRowType, metadataFields) = {
  requiredSchema.fields.foreach(f => checkMetadataColumn(f.name))
  val (_requiredTableFields, _metadataFields) =
    requiredSchema.fields.partition(field => tableRowType.containsField(field.name))
  val _readTableRowType =
    SparkTypeUtils.prunePaimonRowType(StructType(_requiredTableFields), tableRowType)
  (_readTableRowType, _metadataFields)
}
```

Available metadata columns (defined in `PaimonMetadataColumn`):
- `__paimon_file_path` -- the data file path
- `__paimon_row_index` -- the row index within the file
- `__paimon_partition` -- the partition value (struct type)
- `__paimon_bucket` -- the bucket ID
- `_row_id` -- row tracking ID (requires `row-tracking.enabled=true`)
- `_sequence_number` -- sequence number (requires `row-tracking.enabled=true`)

#### Statistics Reporting: `SupportsReportStatistics`

`BaseScan` reports statistics via `PaimonStatistics`, which estimates size and row counts from split metadata. This information is used by Spark's cost-based optimizer.

#### Custom Metrics

```scala
// BaseScan.scala:153-159
override def supportedCustomMetrics: Array[CustomMetric] = {
  Array(
    PaimonNumSplitMetric(),
    PaimonPartitionSizeMetric(),
    PaimonReadBatchTimeMetric(),
    PaimonResultedTableFilesMetric()
  )
}
```

### Bucketed Scan Support: `SupportsReportPartitioning`

`PaimonScan` reports bucketed partitioning for `HASH_FIXED` tables with a single bucket key:

```scala
// PaimonScan.scala:110-114
override def outputPartitioning: Partitioning = {
  extractBucketTransform
    .map(bucket => new KeyGroupedPartitioning(Array(bucket), inputPartitions.size))
    .getOrElse(new UnknownPartitioning(0))
}
```

When bucketed scan is active, splits are grouped by bucket into `PaimonBucketedInputPartition` instances.

### Ordering Reporting: `SupportsReportOrdering`

For bucketed scans with primary key tables, `PaimonScan` reports primary key ordering:

```scala
// PaimonScan.scala:117-158
override def outputOrdering(): Array[SortOrder]
```

This enables Spark to skip unnecessary sorts for merge joins on primary key columns. The ordering is reported only when all input partitions contain at most one split and that split either requires merge read or has a single data file.

## `PaimonBatch`: Split Planning

`PaimonBatch` (`org.apache.paimon.spark.PaimonBatch`) implements Spark's `Batch` interface:

```scala
// PaimonBatch.scala:34-38
override def planInputPartitions(): Array[InputPartition] =
  inputPartitions.map(_.asInstanceOf[InputPartition]).toArray

override def createReaderFactory(): PartitionReaderFactory =
  PaimonPartitionReaderFactory(readBuilder, metadataColumns, blobAsDescriptor)
```

### Bin-Packing Splits: `BinPackingSplits`

`BinPackingSplits` (`org.apache.paimon.spark.read.BinPackingSplits`) rebalances raw-convertible splits across partitions for more balanced task distribution:

- Computes `maxSplitBytes` based on `source.split.target-size`, `spark.sql.files.maxPartitionBytes`, and available parallelism
- Groups data files from multiple `DataSplit` objects into evenly-sized `PaimonInputPartition` instances
- Respects partition and bucket boundaries (files from different partitions/buckets are not mixed within a split)
- Considers `source.split.open-file-cost` for small file overhead
- Only reshuffles `rawConvertible` splits; non-raw splits (requiring merge read) are kept as-is

## Input Partitions

Two types of input partitions:

```scala
// PaimonInputPartition.scala
case class SimplePaimonInputPartition(splits: Seq[Split]) extends PaimonInputPartition

case class PaimonBucketedInputPartition(splits: Seq[Split], bucket: Int)
  extends PaimonInputPartition with HasPartitionKey
```

- `SimplePaimonInputPartition`: Standard partition containing one or more splits
- `PaimonBucketedInputPartition`: For bucketed scans; implements `HasPartitionKey` for Spark's bucket join optimization

## `PaimonPartitionReader`: Row-Level Reading

`PaimonPartitionReader` (`org.apache.paimon.spark.PaimonPartitionReader`) reads data from splits:

```scala
// PaimonPartitionReader.scala:60-61
private lazy val read = readBuilder.newRead().withIOManager(ioManager)
```

### Reading Process

1. Iterates through splits in the input partition
2. For each split, creates a `PaimonRecordReaderIterator` via `read.createReader(split)`
3. Converts Paimon `InternalRow` to Spark `InternalRow` using `SparkInternalRow`
4. Injects metadata columns via row composition
5. Advances to the next split when current split is exhausted

### Task Metrics

Per-task metrics reported:
- `PaimonNumSplitsTaskMetric` -- number of splits in the partition
- `PaimonPartitionSizeTaskMetric` -- total size of splits
- `PaimonReadBatchTimeTaskMetric` -- cumulative batch read time

## Runtime Filtering

`PaimonScan` implements `SupportsRuntimeV2Filtering` for Dynamic Partition Pruning (DPP). When runtime filters arrive from broadcast hash joins, the scan invalidates its cached splits and re-plans with additional partition filters. This enables pruning partitions based on join-time information.

## Streaming Read: `PaimonMicroBatchStream`

`PaimonMicroBatchStream` (`org.apache.paimon.spark.sources.PaimonMicroBatchStream`) implements `MicroBatchStream` and `SupportsTriggerAvailableNow` for Spark Structured Streaming reads:

### Offset Tracking

Uses `PaimonSourceOffset` based on Paimon snapshot IDs to track read progress.

### Rate Limiting

Configurable via connector options:

| Option | Description |
|--------|-------------|
| `read.stream.maxFilesPerTrigger` | Maximum number of files per micro-batch |
| `read.stream.maxBytesPerTrigger` | Maximum bytes per micro-batch |
| `read.stream.maxRowsPerTrigger` | Maximum rows per micro-batch |
| `read.stream.minRowsPerTrigger` | Minimum rows before triggering (requires `maxTriggerDelayMs`) |
| `read.stream.maxTriggerDelayMs` | Maximum delay before triggering (requires `minRowsPerTrigger`) |

### Trigger Available Now

Implements `SupportsTriggerAvailableNow` for one-time batch processing of all available data. When `prepareForTriggerAvailableNow()` is called, it captures the latest offset and the query terminates after consuming data up to that offset.

### Stream Lifecycle

```
initialOffset()
    |
    v
latestOffset(start, limit) -- compute next batch boundary
    |
    v
planInputPartitions(start, end) -- plan splits for the micro-batch
    |
    v
createReaderFactory() -- PaimonPartitionReaderFactory
    |
    v
commit(end) -- mark offset as committed
```

## Changelog Reads

When `read.changelog=true` is set, `SparkSource.loadTable()` wraps the table in an `AuditLogTable`:

```scala
// SparkSource.scala:121-125
if (Options.fromMap(options).get(SparkConnectorOptions.READ_CHANGELOG)) {
  new AuditLogTable(table)
} else {
  table
}
```

The `AuditLogTable` adds a `_rowkind` column to the output, exposing changelog operations (`+I`, `-D`, `+U`, `-U`).

## Complete Data Flow

```
spark.read.format("paimon").load()  /  SELECT * FROM paimon_table
    |
    v
PaimonSparkTableBase.newScanBuilder(options)
    |
    v
PaimonScanBuilder
    |-- pruneColumns(requiredSchema)        [column pruning]
    |-- pushPredicates(predicates)          [filter push-down]
    |-- pushLimit(limit)                    [limit push-down]
    |-- pushAggregation(aggregation)        [aggregation push-down -> PaimonLocalScan]
    |-- pushTopN(orders, limit)             [TopN push-down]
    |
    v
build() --> PaimonScan (or PaimonLocalScan)
    |
    +-- PaimonLocalScan: return pre-computed rows (no I/O needed)
    |
    +-- PaimonScan:
        |-- readBuilder: ReadBuilder with filters, projections, limit, TopN
        |-- inputSplits: ReadBuilder.newScan().plan().splits()
        |-- getInputPartitions(splits):
        |       +-- bucketed: group by bucket -> PaimonBucketedInputPartition
        |       +-- non-bucketed: BinPackingSplits.pack() -> SimplePaimonInputPartition
        |-- outputPartitioning(): KeyGroupedPartitioning (if bucketed)
        |-- outputOrdering(): primary key ordering (if applicable)
        |
        v
    toBatch() --> PaimonBatch
        |-- planInputPartitions() -> Array[PaimonInputPartition]
        |-- createReaderFactory() -> PaimonPartitionReaderFactory
                |
                v
            PaimonPartitionReader (per task)
                |-- iterates splits
                |-- PaimonRecordReaderIterator per split
                |-- Paimon InternalRow -> SparkInternalRow
                |-- metadata column injection
                |-- custom task metrics
```

## Key Source Files

| File | Description |
|------|-------------|
| `paimon-spark-common/.../PaimonSparkTableBase.scala` | Table with `SupportsRead`, `newScanBuilder()` |
| `paimon-spark-common/.../PaimonBaseScanBuilder.scala` | Column pruning, filter/limit push-down |
| `paimon-spark-common/.../PaimonScanBuilder.scala` | Aggregation and TopN push-down, `build()` |
| `paimon-spark-common/.../PaimonScan.scala` | Bucketed scan, partitioning/ordering reporting |
| `paimon-spark-common/.../read/BaseScan.scala` | `ReadBuilder` construction, metadata columns, statistics |
| `paimon-spark-common/.../PaimonBatch.scala` | `Batch` implementation |
| `paimon-spark-common/.../PaimonInputPartition.scala` | Simple and bucketed input partitions |
| `paimon-spark-common/.../PaimonPartitionReaderFactory.scala` | Reader factory |
| `paimon-spark-common/.../PaimonPartitionReader.scala` | Row-level reading, metrics |
| `paimon-spark-common/.../SparkV2FilterConverter.scala` | Spark V2 filter to Paimon predicate conversion |
| `paimon-spark-common/.../read/BinPackingSplits.scala` | Split bin-packing for balanced tasks |
| `paimon-spark-common/.../read/PaimonLocalScan.scala` | Local scan for aggregation push-down |
| `paimon-spark-common/.../sources/PaimonMicroBatchStream.scala` | Streaming micro-batch reads |
| `paimon-spark-common/.../schema/PaimonMetadataColumn.scala` | Metadata column definitions |

## Related Documentation

- [Spark DataSource V1 Read Path](spark-datasource-v1-read.md) -- the V1 read stub
- [Spark DataSource V2 Write Path](spark-datasource-v2-write.md) -- V2 write path
- [Spark DataSource V1 Write Path](spark-datasource-v1-write.md) -- default write path
- [Spark Integration Overview](spark-integration-overview.md) -- high-level integration summary
- [Spark Integration Architecture](spark-integration-architecture.md) -- module structure and shim pattern
- [Spark Integration Configuration](spark-integration-configuration.md) -- connector and catalog options
