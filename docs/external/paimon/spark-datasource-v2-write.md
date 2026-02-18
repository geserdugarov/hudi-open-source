# Spark DataSource V2 Write Path

## Introduction

The V2 write path is **opt-in** via `write.use-v2-write=true` (default: `false`). It is only supported for `HASH_FIXED` and `BUCKET_UNAWARE` bucket modes (and `POSTPONE_MODE` without fixed bucket). When conditions are not met, Paimon falls back to the V1 write path.

V2 write leverages Spark's `RequiresDistributionAndOrdering` interface to let Spark handle data shuffling, rather than implementing custom shuffle logic like the V1 path. Streaming writes always use the V1 path -- `STREAMING_WRITE` capability is not advertised.

## Enablement Check

`PaimonSparkTableBase.useV2Write` determines whether V2 write is used:

```scala
// PaimonSparkTableBase.scala:50-68
lazy val useV2Write: Boolean = {
  val v2WriteConfigured = OptionUtils.useV2Write()
  v2WriteConfigured && supportsV2Write
}

private def supportsV2Write: Boolean = {
  coreOptions.bucketFunctionType() == BucketFunctionType.DEFAULT && {
    table match {
      case storeTable: FileStoreTable =>
        storeTable.bucketMode() match {
          case HASH_FIXED => BucketFunction.supportsTable(storeTable)
          case BUCKET_UNAWARE => true
          case POSTPONE_MODE if !coreOptions.postponeBatchWriteFixedBucket() => true
          case _ => false
        }
      case _ => false
    }
  } && coreOptions.clusteringColumns().isEmpty
}
```

Requirements for V2 write:
1. `write.use-v2-write=true` is configured
2. `BucketFunctionType` is `DEFAULT`
3. Bucket mode is `HASH_FIXED` (with single-key bucket support), `BUCKET_UNAWARE`, or `POSTPONE_MODE` (without fixed bucket)
4. No clustering columns configured

## Table Capabilities

When V2 write is enabled:

```scala
// PaimonSparkTableBase.scala:99-106
if (useV2Write) {
  capabilities.add(TableCapability.ACCEPT_ANY_SCHEMA)
  capabilities.add(TableCapability.BATCH_WRITE)
  capabilities.add(TableCapability.OVERWRITE_DYNAMIC)
} else {
  capabilities.add(TableCapability.ACCEPT_ANY_SCHEMA)
  capabilities.add(TableCapability.V1_BATCH_WRITE)
}
```

V2 write adds `BATCH_WRITE` and `OVERWRITE_DYNAMIC`; V1 write uses `V1_BATCH_WRITE`. Both paths advertise `ACCEPT_ANY_SCHEMA` for schema merge support.

## Entry Point: `newWriteBuilder()`

```scala
// PaimonSparkTableBase.scala:143-155
override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
  table match {
    case fileStoreTable: FileStoreTable =>
      val options = Options.fromMap(info.options)
      if (useV2Write) {
        new PaimonV2WriteBuilder(fileStoreTable, info.schema(), options)
      } else {
        new PaimonWriteBuilder(fileStoreTable, options)
      }
    case _ =>
      throw new RuntimeException("Only FileStoreTable can be written.")
  }
}
```

## WriteBuilder: `PaimonV2WriteBuilder`

`PaimonV2WriteBuilder` (`org.apache.paimon.spark.write.PaimonV2WriteBuilder`) extends `BaseV2WriteBuilder` and provides overwrite and dynamic overwrite support.

### `BaseV2WriteBuilder`

```scala
// BaseV2WriteBuilder.scala:28-63
abstract class BaseV2WriteBuilder(table: Table)
  extends BaseWriteBuilder(table)
  with SupportsOverwrite
  with SupportsDynamicOverwrite
```

#### Partition-Level Overwrite: `SupportsOverwrite`

```scala
override def overwrite(filters: Array[Filter]): WriteBuilder = {
  // Validates filters are partition-only with Equal/EqualNullSafe
  failIfCanNotOverwrite(filters)
  overwriteDynamic = Some(false)
  // Converts filter to partition map or truncation
  overwritePartitions = Some(...)
  this
}
```

Validation ensures only partition columns appear in overwrite filters, and only `Equal`/`EqualNullSafe` operators are used.

#### Dynamic Partition Overwrite: `SupportsDynamicOverwrite`

```scala
override def overwriteDynamicPartitions(): WriteBuilder = {
  overwriteDynamic = Some(true)
  overwritePartitions = Some(Map.empty[String, String])
  this
}
```

#### Copy-on-Write Row-Level Operations

```scala
def overwriteFiles(copyOnWriteScan: PaimonCopyOnWriteScan): WriteBuilder
```

Used for `UPDATE`/`DELETE` operations in copy-on-write mode, where existing files are read, modified, and rewritten.

### Build

```scala
// PaimonV2WriteBuilder.scala:33-40
override def build: PaimonV2Write = {
  val finalTable = overwriteDynamic match {
    case Some(o) =>
      table.copy(Map(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key -> o.toString).asJava)
    case _ => table
  }
  new PaimonV2Write(finalTable, overwritePartitions, copyOnWriteScan, dataSchema, options)
}
```

## `PaimonV2Write`: Distribution and Ordering

`PaimonV2Write` (`org.apache.paimon.spark.write.PaimonV2Write`) implements `Write` and `RequiresDistributionAndOrdering`:

### Schema Merge

```scala
// PaimonV2Write.scala:49
private val writeSchema = mergeSchema(dataSchema, options)
```

Schema merge on the V2 path uses `SchemaHelper.mergeSchema(dataSchema, options)`, which handles `write.merge-schema` and `write.merge-schema.explicit-cast` options.

### Distribution Requirements: `PaimonWriteRequirement`

`PaimonWriteRequirement` (`org.apache.paimon.spark.write.PaimonWriteRequirement`) computes the required data distribution:

```scala
// PaimonWriteRequirement.scala:40-73
def apply(table: FileStoreTable): PaimonWriteRequirement = {
  val bucketTransforms = bucketSpec.getBucketMode match {
    case HASH_FIXED =>
      Seq(Expressions.bucket(bucketSpec.getNumBuckets, bucketKeys...))
    case BUCKET_UNAWARE | POSTPONE_MODE =>
      Seq.empty
    case _ =>
      throw new UnsupportedOperationException(...)
  }

  val partitionTransforms =
    table.schema().partitionKeys().map(key => Expressions.identity(key))
  val clusteringExpressions =
    (partitionTransforms ++ bucketTransforms).toArray

  if (clusteringExpressions.isEmpty || ...) {
    EMPTY  // Distributions.unspecified()
  } else {
    PaimonWriteRequirement(Distributions.clustered(clusteringExpressions), EMPTY_ORDERING)
  }
}
```

| Bucket Mode | Partitioned | Distribution |
|-------------|-------------|-------------|
| `HASH_FIXED` | Yes | `ClusteredDistribution(partition_keys + bucket(N, bucket_keys))` |
| `HASH_FIXED` | No | `ClusteredDistribution(bucket(N, bucket_keys))` |
| `BUCKET_UNAWARE` | Yes | `ClusteredDistribution(partition_keys)` (unless `partition.sink-strategy=none`) |
| `BUCKET_UNAWARE` | No | `Distributions.unspecified()` |
| `POSTPONE_MODE` | Yes | `ClusteredDistribution(partition_keys)` |
| `POSTPONE_MODE` | No | `Distributions.unspecified()` |

Ordering is always empty (`Array.empty[SortOrder]`).

### Custom Metrics

```scala
// PaimonV2Write.scala:68-93
override def supportedCustomMetrics(): Array[CustomMetric]
```

Reported metrics include:
- `PaimonNumWritersMetric` -- number of data writers
- `PaimonCommitDurationMetric` -- commit duration
- `PaimonAddedTableFilesMetric` -- number of added files
- `PaimonInsertedRecordsMetric` -- number of inserted records (not for copy-on-write)
- `PaimonDeletedTableFilesMetric` -- number of deleted files (copy-on-write only)
- `PaimonAppendedChangelogFilesMetric` -- changelog files (when changelog producer is enabled)
- `PaimonPartitionsWrittenMetric` -- number of partitions written (partitioned tables)
- `PaimonBucketsWrittenMetric` -- number of buckets written (bucketed tables)

### Batch Write Creation

```scala
// PaimonV2Write.scala:64-66
override def toBatch: BatchWrite = {
  PaimonBatchWrite(table, writeSchema, dataSchema, overwritePartitions, copyOnWriteScan)
}
```

## `PaimonBatchWrite`: Task Coordination

`PaimonBatchWrite` (`org.apache.paimon.spark.write.PaimonBatchWrite`) implements Spark's `BatchWrite`:

### Writer Factory

```scala
// PaimonBatchWrite.scala:56-66
override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
  (_: Int, _: Long) => {
    PaimonV2DataWriter(
      batchWriteBuilder, writeSchema, dataSchema,
      coreOptions, table.catalogEnvironment().catalogContext())
  }
}
```

Each Spark task gets its own `PaimonV2DataWriter` instance.

### Commit Coordination

```scala
override def useCommitCoordinator(): Boolean = false
```

No commit coordinator is needed -- each writer independently produces `CommitMessage` objects.

### Commit

```scala
// PaimonBatchWrite.scala:70-89
override def commit(messages: Array[WriterCommitMessage]): Unit = {
  val batchTableCommit = batchWriteBuilder.newCommit()
  batchTableCommit.withMetricRegistry(metricRegistry)
  val addCommitMessage = WriteTaskResult.merge(messages)
  val deletedCommitMessage = copyOnWriteScan match {
    case Some(scan) => buildDeletedCommitMessage(scan.scannedFiles)
    case None => Seq.empty
  }
  val commitMessages = addCommitMessage ++ deletedCommitMessage
  try {
    batchTableCommit.commit(commitMessages.asJava)
  } finally {
    batchTableCommit.close()
  }
  postDriverMetrics()
  postCommit(commitMessages)
}
```

Commit steps:
1. Merges `WriteTaskResult` objects from all writer tasks
2. For copy-on-write operations, builds deleted file commit messages from the scan's scanned files
3. Commits all messages via `BatchWriteBuilder.newCommit().commit()`
4. Posts driver-side metrics to Spark's metric system
5. Performs post-commit actions (HMS reporting, tag creation, partition mark-done) via `WriteHelper`

### Abort

```scala
override def abort(messages: Array[WriterCommitMessage]): Unit = {
  // TODO clean uncommitted files
}
```

Abort cleanup is not yet implemented.

## `PaimonV2DataWriter`: Per-Task Writing

`PaimonV2DataWriter` (`org.apache.paimon.spark.write.PaimonV2DataWriter`) implements Spark's `DataWriter[InternalRow]`:

### Initialization

```scala
// PaimonV2DataWriter.scala:50-56
val write: TableWriteImpl[InternalRow] = {
  writeBuilder
    .newWrite()
    .withIOManager(ioManager)
    .withMetricRegistry(metricRegistry)
    .asInstanceOf[TableWriteImpl[InternalRow]]
}
```

### Row Conversion

```scala
// PaimonV2DataWriter.scala:58-67
private val rowConverter: InternalRow => SparkInternalRowWrapper = {
  val numFields = writeSchema.fields.length
  val reusableWrapper = new SparkInternalRowWrapper(
    writeSchema, numFields, dataSchema,
    blobAsDescriptor, catalogContext)
  record => reusableWrapper.replace(record)
}
```

`SparkInternalRowWrapper` (`org.apache.paimon.spark.SparkInternalRowWrapper`) wraps a Spark `InternalRow` as a Paimon `InternalRow`. It handles:
- Field index mapping for schema evolution (when `writeSchema` differs from `dataSchema`)
- Blob descriptor support

### Write and Commit

```scala
// PaimonV2DataWriter.scala:69-75
override def write(record: InternalRow): Unit = {
  postWrite(write.writeAndReturn(rowConverter.apply(record)))
}

override def commitImpl(): Seq[CommitMessage] = {
  write.prepareCommit().asScala.toSeq
}
```

### Task Metrics

```scala
override def currentMetricsValues(): Array[CustomTaskMetric] = {
  metricRegistry.buildSparkWriteMetrics()
}
```

Reports per-task write metrics via `SparkMetricRegistry`.

## Streaming Write

V2 streaming write is **NOT supported**. The `STREAMING_WRITE` capability is not advertised by `PaimonSparkTableBase`. All streaming writes use the V1 path via `PaimonSink`/`StreamSinkProvider` (see [Spark DataSource V1 Write Path](spark-datasource-v1-write.md#streaming-write-paimonsink)).

## Complete Data Flow

```
INSERT INTO / df.writeTo(table).append() / OVERWRITE
    |
    v
PaimonSparkTableBase.newWriteBuilder(info)
    |  (useV2Write == true)
    v
PaimonV2WriteBuilder(table, dataSchema, options)
    |-- overwrite(filters)              [partition-level overwrite]
    |-- overwriteDynamicPartitions()    [dynamic partition overwrite]
    |-- overwriteFiles(scan)            [copy-on-write UPDATE/DELETE]
    |
    v
build() --> PaimonV2Write
    |-- mergeSchema(dataSchema, options)  [schema evolution]
    |-- requiredDistribution():
    |       HASH_FIXED: ClusteredDistribution(partitions + bucket(N, keys))
    |       BUCKET_UNAWARE: ClusteredDistribution(partitions) or unspecified
    |-- requiredOrdering(): empty
    |-- supportedCustomMetrics()
    |
    v
toBatch() --> PaimonBatchWrite
    |-- createBatchWriterFactory() --> lambda creating PaimonV2DataWriter per task
    |       |
    |       v
    |   PaimonV2DataWriter (per Spark task)
    |       |-- write(record): Spark InternalRow -> SparkInternalRowWrapper -> write.writeAndReturn()
    |       |-- commit(): write.prepareCommit() -> WriteTaskResult
    |       |-- currentMetricsValues(): per-task write metrics
    |
    |-- useCommitCoordinator() -> false
    |
    |-- commit(messages):
    |       +-- WriteTaskResult.merge(messages)
    |       +-- buildDeletedCommitMessage() (for copy-on-write)
    |       +-- BatchWriteBuilder.newCommit().commit(allMessages)
    |       +-- postDriverMetrics()
    |       +-- postCommit():
    |               +-- reportToHms()
    |               +-- batchCreateTag()
    |               +-- markDoneIfNeeded()
    |
    v
  Done
```

## Key Source Files

| File | Description |
|------|-------------|
| `paimon-spark-common/.../PaimonSparkTableBase.scala` | `useV2Write` check, `newWriteBuilder()`, capabilities |
| `paimon-spark-common/.../write/PaimonV2WriteBuilder.scala` | V2 write builder |
| `paimon-spark-common/.../write/BaseV2WriteBuilder.scala` | Overwrite/dynamic overwrite/copy-on-write support |
| `paimon-spark-common/.../write/BaseWriteBuilder.scala` | Filter validation for overwrite operations |
| `paimon-spark-common/.../write/PaimonV2Write.scala` | Distribution/ordering requirements, metrics, schema merge |
| `paimon-spark-common/.../write/PaimonBatchWrite.scala` | Writer factory, commit logic, driver metrics |
| `paimon-spark-common/.../write/PaimonV2DataWriter.scala` | Per-task row writing, row conversion |
| `paimon-spark-common/.../write/PaimonWriteRequirement.scala` | Distribution computation per bucket mode |
| `paimon-spark-common/.../write/WriteHelper.scala` | Post-commit actions (HMS, tagging, mark-done) |
| `paimon-spark-common/.../SparkInternalRowWrapper.java` | Spark-to-Paimon row adapter |

## Related Documentation

- [Spark DataSource V1 Write Path](spark-datasource-v1-write.md) -- the default write path
- [Spark DataSource V2 Read Path](spark-datasource-v2-read.md) -- the read implementation
- [Spark DataSource V1 Read Path](spark-datasource-v1-read.md) -- V1 read stub
- [Spark Integration Overview](spark-integration-overview.md) -- high-level integration summary
- [Spark Integration Architecture](spark-integration-architecture.md) -- module structure and shim pattern
- [Spark Integration Configuration](spark-integration-configuration.md) -- connector and catalog options
