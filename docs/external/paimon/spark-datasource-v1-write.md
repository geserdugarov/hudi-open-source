# Spark DataSource V1 Write Path

## Introduction

The V1 write path is the **default** write path for Paimon in Spark. The V2 write path is opt-in via the `write.use-v2-write` option. This document covers the complete V1 write flow for both batch and streaming writes.

When `useV2Write` is false (default), `PaimonSparkTableBase` advertises the `V1_BATCH_WRITE` capability, causing Spark to use the V1 write protocol.

## Entry Points

There are two V1 write entry points depending on how the write is initiated:

### 1. DataFrame API Path: `SparkSource.createRelation()`

When using `df.write.format("paimon").mode(...).save()`, Spark calls `CreatableRelationProvider.createRelation()`:

```scala
// SparkSource.scala:74-83
override def createRelation(
    sqlContext: SQLContext,
    mode: SparkSaveMode,
    parameters: Map[String, String],
    data: DataFrame): BaseRelation = {
  val table = loadTable(parameters.asJava).asInstanceOf[FileStoreTable]
  WriteIntoPaimonTable(table, SaveMode.transform(mode), data, Options.fromMap(parameters.asJava))
    .run(sqlContext.sparkSession)
  SparkSource.toBaseRelation(table, sqlContext)
}
```

### 2. Catalog/SQL Path: `PaimonWriteBuilder` -> `PaimonWrite` (V1Write)

When using SQL (`INSERT INTO/OVERWRITE`) or catalog-based writes, Spark calls `PaimonSparkTableBase.newWriteBuilder()`, which creates a `PaimonWriteBuilder`. Its `build()` method returns a `PaimonWrite` instance, which implements Spark's `V1Write` interface:

```scala
// PaimonWrite.scala:31-37
class PaimonWrite(val table: FileStoreTable, saveMode: SaveMode, options: Options) extends V1Write {
  override def toInsertableRelation: InsertableRelation = {
    (data: DataFrame, overwrite: Boolean) => {
      WriteIntoPaimonTable(table, saveMode, data, options).run(data.sparkSession)
    }
  }
}
```

Both entry points converge on `WriteIntoPaimonTable`, the core write command.

## SaveMode Mapping

Spark's `SaveMode` values are mapped to Paimon's internal `SaveMode` types:

```scala
// SaveMode.scala:37-44
object SaveMode {
  def transform(saveMode: SparkSaveMode): SaveMode = {
    saveMode match {
      case SparkSaveMode.Overwrite => Overwrite(Some(AlwaysTrue))
      case SparkSaveMode.Ignore => Ignore
      case SparkSaveMode.Append => InsertInto
      case SparkSaveMode.ErrorIfExists => ErrorIfExists
    }
  }
}
```

| Spark SaveMode | Paimon SaveMode | Behavior |
|----------------|-----------------|----------|
| `Append` | `InsertInto` | Insert data without overwriting |
| `Overwrite` | `Overwrite(AlwaysTrue)` | Overwrite all data |
| `Ignore` | `Ignore` | Skip write if table exists |
| `ErrorIfExists` | `ErrorIfExists` | Fail if table exists |

Additionally, `DynamicOverWrite` is used for dynamic partition overwrite (when `spark.sql.sources.partitionOverwriteMode=dynamic`), and `Overwrite(filter)` supports partition-level overwrite with specific filters.

## `WriteIntoPaimonTable` Command

`WriteIntoPaimonTable` (`org.apache.paimon.spark.commands.WriteIntoPaimonTable`) is a Spark `RunnableCommand` that orchestrates the write process.

### Execution Flow

```scala
// WriteIntoPaimonTable.scala:46-62
override def run(sparkSession: SparkSession): Seq[Row] = {
  val data = mergeSchema(sparkSession, _data, options)
  val (dynamicPartitionOverwriteMode, overwritePartition) = parseSaveMode()
  updateTableWithOptions(
    Map(DYNAMIC_PARTITION_OVERWRITE.key -> dynamicPartitionOverwriteMode.toString))
  val writer = PaimonSparkWriter(table, batchId = batchId)
  if (overwritePartition != null) {
    writer.writeBuilder.withOverwrite(overwritePartition.asJava)
  }
  val commitMessages = writer.write(data)
  writer.commit(commitMessages)
  Seq.empty
}
```

Steps:
1. **Schema merge**: `mergeSchema()` from `SchemaHelper` handles `write.merge-schema` and `write.merge-schema.explicit-cast` options, evolving the table schema if needed.
2. **SaveMode parsing**: `parseSaveMode()` determines `dynamicPartitionOverwriteMode` and `overwritePartition` map from the `SaveMode`.
3. **Writer creation**: `PaimonSparkWriter` is instantiated with the table.
4. **Overwrite configuration**: If overwriting, the partition filter map is set on the `BatchWriteBuilder`.
5. **Data writing**: `writer.write(data)` distributes and writes data based on bucket mode.
6. **Commit**: `writer.commit(commitMessages)` commits the write transaction.

## `PaimonSparkWriter`: Bucket-Mode-Specific Writing

`PaimonSparkWriter` (`org.apache.paimon.spark.commands.PaimonSparkWriter`) handles data distribution/shuffling and per-partition writing. The write strategy varies by bucket mode:

### Bucket Mode Strategies

| Bucket Mode | Topology | Description |
|-------------|----------|-------------|
| `HASH_FIXED` | input -> bucket-assigner -> shuffle by partition & bucket | Assigns fixed bucket IDs using hash function, then repartitions by partition keys + bucket |
| `HASH_FIXED` (with extension) | input -> shuffle by partition & bucket | Uses `BucketFunction` UDF directly, then repartitions |
| `KEY_DYNAMIC` | input -> bootstrap -> shuffle by key hash -> bucket-assigner -> shuffle by partition & bucket | Cross-partition mode with index bootstrap for global bucket assignment |
| `HASH_DYNAMIC` (bootstrap) | input -> shuffle by key & partition hash -> bucket-assigner | First write uses `SimpleHashBucketAssigner` |
| `HASH_DYNAMIC` (incremental) | input -> shuffle by key & partition hash -> bucket-assigner -> shuffle by partition & bucket | Subsequent writes use `DynamicBucketProcessor` |
| `BUCKET_UNAWARE` | input -> write (no shuffle) | No bucket concept; optional partition-based repartition if `partition.sink-strategy=hash` |
| `POSTPONE_MODE` | input -> write (no shuffle) | Similar to `BUCKET_UNAWARE` for the write path |
| `POSTPONE_MODE` (fixed bucket) | input -> bucket-assigner -> shuffle by partition & bucket | Uses `PostponeFixBucketProcessor` |

### Per-Partition Writing with `PaimonDataWrite`

Each Spark partition creates a `PaimonDataWrite` instance via `mapPartitions`:

```scala
// PaimonSparkWriter.scala:152-165
def writeWithoutBucket(dataFrame: DataFrame) = {
  dataFrame.mapPartitions {
    iter => {
      val write = newWrite()
      try {
        iter.foreach(row => write.write(row))
        Iterator.apply(write.commit)
      } finally {
        write.close()
      }
    }
  }
}
```

## `PaimonDataWrite`: Row-Level Writing

`PaimonDataWrite` (`org.apache.paimon.spark.write.PaimonDataWrite`) wraps Paimon's `BatchWriteBuilder.newWrite()` and handles individual row writing:

```scala
// PaimonDataWrite.scala:64-70
def write(row: Row): Unit = {
  postWrite(write.writeAndReturn(toPaimonRow(row)))
}

def write(row: Row, bucket: Int): Unit = {
  postWrite(write.writeAndReturn(toPaimonRow(row), bucket))
}
```

Key components:
- **Row conversion**: `SparkRowUtils.toPaimonRow()` converts Spark `Row` to Paimon `InternalRow`
- **Write handle**: `TableWriteImpl[Row]` with an `IOManager` for disk spilling
- **Commit output**: `write.prepareCommit()` produces `CommitMessage` objects per task

## Commit Protocol

`PaimonSparkWriter.commit()` collects `CommitMessage` objects from all tasks and commits the transaction:

```scala
// PaimonSparkWriter.scala:412-431
def commit(commitMessages: Seq[CommitMessage]): Unit = {
  val tableCommit = finalWriteBuilder.newCommit()
  try {
    tableCommit.commit(commitMessages.toList.asJava)
  } catch {
    case e: Throwable => throw new RuntimeException(e);
  } finally {
    tableCommit.close()
  }
  postCommit(commitMessages)
}
```

### Post-Commit Actions (`WriteHelper` Trait)

After a successful commit, `postCommit()` performs optional actions:

1. **HiveMetastore reporting** (`reportToHms`): Reports partition statistics if configured with `partition.idle-time-to-report-statistic` and if the table is partitioned with a metastore.
2. **Batch tag creation** (`batchCreateTag`): Creates a tag for the new snapshot if `tag.creation-mode=batch`.
3. **Partition mark-done** (`markDoneIfNeeded`): Marks partitions as done if `partition.mark-done-when-end-input=true`.

## Streaming Write: `PaimonSink`

For Spark Structured Streaming, `SparkSource.createSink()` creates a `PaimonSink` (`org.apache.paimon.spark.sources.PaimonSink`) via the `StreamSinkProvider` interface:

```scala
// PaimonSink.scala:40-49
override def addBatch(batchId: Long, data: DataFrame): Unit = {
  val saveMode = if (outputMode == OutputMode.Complete()) {
    Overwrite(Some(AlwaysTrue))
  } else {
    InsertInto
  }
  val newData = PaimonUtils.createNewDataFrame(data)
  WriteIntoPaimonTable(originTable, saveMode, newData, options, Some(batchId)).run(
    sqlContext.sparkSession)
}
```

- **Append mode** -> `InsertInto` save mode
- **Complete mode** -> `Overwrite(AlwaysTrue)` (full table replacement per micro-batch)
- Each micro-batch executes a full `WriteIntoPaimonTable` command with a `batchId` for idempotency
- Only `Append` and `Complete` output modes are supported

## Complete Data Flow

```
DataFrame.write / INSERT INTO / Structured Streaming
    |
    v
SparkSource.createRelation()  OR  PaimonWriteBuilder.build() -> PaimonWrite (V1Write)
    |                                       |
    +---------------------------------------+
    |
    v
WriteIntoPaimonTable.run(sparkSession)
    |
    +-- mergeSchema() -- schema evolution if write.merge-schema=true
    |
    +-- parseSaveMode() -- determine overwrite mode
    |
    +-- PaimonSparkWriter(table)
    |       |
    |       +-- write(data): bucket-mode-specific shuffle/repartition
    |       |       |
    |       |       +-- HASH_FIXED: hash-based bucket assignment + repartition
    |       |       +-- KEY_DYNAMIC: bootstrap + global bucket assignment
    |       |       +-- HASH_DYNAMIC: partition-key hash + local bucket assignment
    |       |       +-- BUCKET_UNAWARE: direct write (optional partition repartition)
    |       |       +-- POSTPONE_MODE: direct write or fixed-bucket assignment
    |       |       |
    |       |       +-- mapPartitions:
    |       |               PaimonDataWrite.write(row) per row
    |       |               PaimonDataWrite.commit -> CommitMessage per task
    |       |
    |       +-- commit(commitMessages)
    |               |
    |               +-- BatchWriteBuilder.newCommit().commit(messages)
    |               +-- postCommit():
    |                       +-- reportToHms() -- HiveMetastore partition stats
    |                       +-- batchCreateTag() -- snapshot tagging
    |                       +-- markDoneIfNeeded() -- partition mark-done
    |
    v
  Done
```

## Key Source Files

| File | Description |
|------|-------------|
| `paimon-spark-common/.../SparkSource.scala` | `CreatableRelationProvider` and `StreamSinkProvider` entry points |
| `paimon-spark-common/.../SaveMode.scala` | Spark-to-Paimon save mode mapping |
| `paimon-spark-common/.../commands/WriteIntoPaimonTable.scala` | Core write command |
| `paimon-spark-common/.../commands/SchemaHelper.scala` | Schema merge logic |
| `paimon-spark-common/.../commands/PaimonSparkWriter.scala` | Bucket-mode-specific data distribution and writing |
| `paimon-spark-common/.../write/PaimonDataWrite.scala` | Per-task row-level writing |
| `paimon-spark-common/.../write/PaimonWrite.scala` | V1Write implementation |
| `paimon-spark-common/.../write/PaimonWriteBuilder.scala` | V1 write builder (catalog/SQL path) |
| `paimon-spark-common/.../write/WriteHelper.scala` | Post-commit actions (HMS, tagging, mark-done) |
| `paimon-spark-common/.../sources/PaimonSink.scala` | Streaming write sink |

## Related Documentation

- [Spark DataSource V2 Write Path](spark-datasource-v2-write.md) -- opt-in V2 write path
- [Spark DataSource V1 Read Path](spark-datasource-v1-read.md) -- V1 read stub
- [Spark Integration Overview](spark-integration-overview.md) -- high-level integration summary
- [Spark Integration Architecture](spark-integration-architecture.md) -- module structure and shim pattern
