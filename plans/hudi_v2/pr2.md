# PR2: CoW Snapshot Read — File Listing and Base File Reading

## Context

PR1 (DONE, branch `hudi-worktree-v2`) established DSv2/DSv1 coexistence with stub reads that return empty results. PR2 wires actual CoW snapshot reading so that `format("hudi_v2")` and `SELECT` with `hoodie.datasource.read.use.v2=true` read real data from Copy-on-Write tables.

The approach reuses the existing `HoodieFileIndex` for file listing and `SparkColumnarFileReader` (Spark's native Parquet reader) for base file reading — the same components used by the DSv1 `HoodieCopyOnWriteSnapshotHadoopFsRelationFactory` path.

## Data Flow

```
HoodieSparkV2Table.newScanBuilder(options)
  → passes spark, metaClient, tableSchema, options

HoodieScanBuilder
  ← pruneColumns(requiredSchema)       [from Spark optimizer]
  ← pushFilters(filters)               [stub: all post-scan for now]
  → build():
    1. Create HoodieFileIndex(spark, metaClient, None, options)
    2. Split requiredSchema into requiredDataSchema + requiredPartitionSchema
       using fileIndex.partitionSchema
    3. Create SparkColumnarFileReader via sparkAdapter (non-vectorized)
    4. Broadcast reader + SerializableConfiguration
    5. Enumerate file slices via fileIndex.filterFileSlices(Seq.empty, Seq.empty)
    6. Map each (PartitionPath, FileSlice) → HoodieInputPartition
    7. Return HoodieBatchScan(readSchema, partitions, broadcasts, schemas)

HoodieBatchScan
  → planInputPartitions() returns pre-computed partitions
  → createReaderFactory() returns HoodiePartitionReaderFactory(broadcasts, schemas)

HoodiePartitionReaderFactory.createReader(partition)
  → HoodiePartitionReader(partition, broadcasts, schemas)

HoodiePartitionReader (executor)
  1. Build PartitionedFile from base file path + partition values
  2. Call SparkColumnarFileReader.read(file, dataSchema, partSchema, ...)
  3. Project to readSchema order if needed (data+part → readSchema)
  4. Return Iterator[InternalRow]
```

## File Changes

### 1. `HoodieSparkV2Table.scala`
**Path:** `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieSparkV2Table.scala`

Change `newScanBuilder()` to pass `spark`, `metaClient`, and merged options:
```scala
override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
  val mergedOpts: Map[String, String] = ... // merge options with table properties
  new HoodieScanBuilder(spark, metaClient, tableSchema, mergedOpts)
}
```

### 2. `HoodieScanBuilder.scala`
**Path:** `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieScanBuilder.scala`

Major rewrite. New constructor parameters: `spark: SparkSession`, `metaClient: HoodieTableMetaClient`, `tableSchema: StructType`, `options: Map[String, String]`.

`build()` method does the heavy lifting:

1. **Create HoodieFileIndex:**
   ```scala
   val fileIndex = HoodieFileIndex(spark, metaClient, None, options,
     includeLogFiles = false, shouldEmbedFileSlices = false)
   ```
   - `schemaSpec = None` → resolves full schema from metaClient internally
   - `includeLogFiles = false` → CoW only
   - Reuse: `org.apache.hudi.HoodieFileIndex` (existing)

2. **Split requiredSchema** into data vs partition:
   ```scala
   val partFieldNames = fileIndex.partitionSchema.fieldNames.toSet
   val requiredDataSchema = StructType(requiredSchema.filterNot(f => partFieldNames(f.name)))
   val requiredPartitionSchema = StructType(requiredSchema.filter(f => partFieldNames(f.name)))
   ```

3. **Create and broadcast Parquet reader** (non-vectorized):
   ```scala
   val reader = sparkAdapter.createParquetFileReader(false, spark.sessionState.conf, options, hadoopConf)
   val broadcastReader = spark.sparkContext.broadcast(reader)
   ```
   - Reuse: `SparkAdapterSupport.sparkAdapter.createParquetFileReader()` (existing)

4. **Broadcast Hadoop config:**
   ```scala
   val broadcastConf = spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
   ```

5. **Enumerate file slices:**
   ```scala
   val fileSlicesPerPartition = fileIndex.filterFileSlices(Seq.empty, Seq.empty)
   ```
   Returns `Seq[(Option[PartitionPath], Seq[FileSlice])]`.
   Map to `Array[HoodieInputPartition]` — one per base file.
   Extract partition values via `partitionPath.getValues` (returns `Object[]` in Spark internal format).
   For `requiredPartitionSchema`, extract only the values for required partition columns (index-map from `fileIndex.partitionSchema`).

6. **Build HoodieBatchScan** with readSchema, partitions, broadcasts, and schema info.

### 3. `HoodieBatchScan.scala`
**Path:** `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieBatchScan.scala`

New constructor:
```scala
class HoodieBatchScan(
    readSchema: StructType,
    inputPartitions: Array[InputPartition],
    broadcastReader: Broadcast[SparkColumnarFileReader],
    broadcastConf: Broadcast[SerializableConfiguration],
    requiredDataSchema: StructType,
    requiredPartitionSchema: StructType) extends Scan with Batch
```

- `planInputPartitions()` → returns `inputPartitions`
- `createReaderFactory()` → creates `HoodiePartitionReaderFactory` with broadcasts + schemas

### 4. `HoodieInputPartition.scala`
**Path:** `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieInputPartition.scala`

Replace placeholder with:
```scala
case class HoodieInputPartition(
    index: Int,
    baseFilePath: String,
    baseFileLength: Long,
    partitionValues: Array[AnyRef]   // Spark internal types, in requiredPartitionSchema order
) extends InputPartition
```

All fields are Java-serializable (`String`, `Long`, `Array[AnyRef]` of `UTF8String`/boxed primitives).

### 5. `HoodiePartitionReaderFactory.scala`
**Path:** `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodiePartitionReaderFactory.scala`

New constructor:
```scala
class HoodiePartitionReaderFactory(
    broadcastReader: Broadcast[SparkColumnarFileReader],
    broadcastConf: Broadcast[SerializableConfiguration],
    readSchema: StructType,
    requiredDataSchema: StructType,
    requiredPartitionSchema: StructType) extends PartitionReaderFactory
```

`createReader()` → creates `HoodiePartitionReader` with the partition + broadcasts + schemas.

### 6. `HoodiePartitionReader.scala`
**Path:** `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodiePartitionReader.scala`

Full rewrite from stub. Constructor takes partition info, broadcast reader/conf, and schemas.

**`createIterator()` logic:**

1. Build `InternalRow` from `partition.partitionValues` for `PartitionedFile`:
   ```scala
   val partValues = InternalRow.fromSeq(partition.partitionValues.toSeq)
   ```

2. Create `PartitionedFile`:
   ```scala
   val pFile = sparkAdapter.getSparkPartitionedFileUtils
     .createPartitionedFile(partValues, new StoragePath(partition.baseFilePath), 0L, partition.baseFileLength)
   ```

3. Read via `SparkColumnarFileReader.read()`:
   ```scala
   val storageConf = new HadoopStorageConfiguration(broadcastConf.value.value)
   val rawIter = broadcastReader.value.read(
     pFile, requiredDataSchema, requiredPartitionSchema,
     HOption.empty(), Seq.empty, storageConf)
   ```
   Returns `Iterator[Any]` (for non-vectorized = `Iterator[InternalRow]`).

4. **Project to readSchema order** if reader output order differs:
   ```scala
   val readerOutputSchema = StructType(requiredDataSchema.fields ++ requiredPartitionSchema.fields)
   if (readerOutputSchema != readSchema) {
     val projection = generateUnsafeProjection(readerOutputSchema, readSchema)
     rawIter.map(row => projection(row.asInstanceOf[InternalRow]))
   } else {
     rawIter.asInstanceOf[Iterator[InternalRow]]
   }
   ```
   - Reuse: `HoodieCatalystExpressionUtils.generateUnsafeProjection()` (existing)

5. Handle empty base file (no base file in slice): return `Iterator.empty`.

6. `close()`: close underlying iterator if `Closeable`.

### Key Reused Utilities

| Utility | Location | Usage |
|---------|----------|-------|
| `HoodieFileIndex` | `org.apache.hudi.HoodieFileIndex` | File listing with partition pruning |
| `HoodieFileIndex.filterFileSlices()` | Same | Get `(PartitionPath, Seq[FileSlice])` |
| `sparkAdapter.createParquetFileReader()` | `SparkAdapterSupport` | Create Parquet base file reader |
| `sparkAdapter.getSparkPartitionedFileUtils` | `SparkAdapterSupport` | Create `PartitionedFile` |
| `SerializableConfiguration` | `org.apache.spark.util` | Broadcast Hadoop config |
| `generateUnsafeProjection()` | `HoodieCatalystExpressionUtils` | Schema reordering projection |
| `HadoopStorageConfiguration` | `org.apache.hudi.storage.hadoop` | Wrap Hadoop config for Hudi storage API |

## New Test File

**Path:** `hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/spark/sql/hudi/v2/TestDSv2CowSnapshotRead.scala`

Extends `SparkClientFunctionalTestHarness`, `@Tag("functional")`.

### Test Cases

1. **testCowReadViaDataFrameApi** — Write 3 rows via DSv1, read via `format("hudi_v2")`. Assert count=3, values match DSv1 read.

2. **testCowReadViaSqlCatalog** — CREATE TABLE, INSERT, set `use.v2=true`, `SELECT *`. Assert count and values match `use.v2=false`.

3. **testColumnPruning** — `SELECT id, name` via DSv2. Verify only 2 columns in result, correct values.

4. **testNonPartitionedTable** — Write non-partitioned CoW table, read via DSv2, verify data.

5. **testPartitionedTable** — Write partitioned CoW table (e.g. by `country`), read via DSv2, verify partition column values correct.

6. **testMultipleCommits** — Write batch 1, write batch 2 (updates), read via DSv2. Verify latest snapshot only.

7. **testDsv1VsDsv2ResultComparison** — Read same table via both paths, compare results (drop meta-fields from DSv1 result).

8. **testSelectPartitionColumnOnly** — `SELECT partCol FROM ...` via DSv2. Verify values.

9. **testMultiplePartitions** — Write data across 3+ partition values, read via DSv2, verify all partitions present.

## Updated Test File

**Path:** `hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/spark/sql/hudi/v2/TestDSv2CoexistenceWithDSv1.scala`

Update assertions that checked for empty stub results:
- `testDSv1WriteAndDSv2Read`: change `assertEquals(0, ...)` to `assertEquals(3, ...)` + verify values
- `testDSv2ReadSchemaAndPlan`: verify schema AND data
- `testSqlConfigTrueUsesDSv2Stub`: rename (no longer stub), change empty→real data assertions
- `testSqlSwitchBetweenV1AndV2Reads`: both paths return real data now

## Implementation Order

1. `HoodieInputPartition.scala` — expand case class (no deps)
2. `HoodiePartitionReader.scala` — implement reader (depends on InputPartition)
3. `HoodiePartitionReaderFactory.scala` — accept broadcasts (depends on Reader)
4. `HoodieBatchScan.scala` — accept partitions + broadcasts (depends on Factory)
5. `HoodieScanBuilder.scala` — orchestrator: FileIndex + broadcasts + partitions (depends on BatchScan)
6. `HoodieSparkV2Table.scala` — pass spark/metaClient/options (depends on ScanBuilder)
7. `TestDSv2CowSnapshotRead.scala` — new test file
8. `TestDSv2CoexistenceWithDSv1.scala` — update existing assertions
9. Update `plans/hudi_v2/incremental_pr_plan.md` — mark PR2 status

## Verification

```bash
# Load environment
export $(grep -v '^#' .env | xargs)

# Build changed modules
JAVA_HOME=${JAVA_HOME_PATH} mvn clean package -DskipTests \
  -Dmaven.repo.local=${MAVEN_REPO_PATH} \
  -Djava11 -Djava.version=11 -Dspark3.5 -Dflink1.20 \
  -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true \
  -Dmaven.wagon.httpconnectionManager.ttlSeconds=25 -Dmaven.wagon.http.retryHandler.count=5 \
  -pl hudi-spark-datasource/hudi-spark-common,hudi-spark-datasource/hudi-spark -am

# Run new CoW snapshot read test
JAVA_HOME=${JAVA_HOME_PATH} mvn test -Pfunctional-tests \
  -Dmaven.repo.local=${MAVEN_REPO_PATH} \
  -Djava11 -Djava.version=11 -Dspark3.5 -Dflink1.20 \
  -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true \
  -Dmaven.wagon.httpconnectionManager.ttlSeconds=25 -Dmaven.wagon.http.retryHandler.count=5 \
  -pl hudi-spark-datasource/hudi-spark \
  -Dtest=TestDSv2CowSnapshotRead

# Run updated coexistence test
JAVA_HOME=${JAVA_HOME_PATH} mvn test -Pfunctional-tests \
  -Dmaven.repo.local=${MAVEN_REPO_PATH} \
  -Djava11 -Djava.version=11 -Dspark3.5 -Dflink1.20 \
  -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true \
  -Dmaven.wagon.httpconnectionManager.ttlSeconds=25 -Dmaven.wagon.http.retryHandler.count=5 \
  -pl hudi-spark-datasource/hudi-spark \
  -Dtest=TestDSv2CoexistenceWithDSv1
```

## Key Risks

1. **Partition value serialization** — `PartitionPath.getValues()` returns `Object[]` with Spark internal types (`UTF8String`, etc.). These are serializable. Mitigated by tests with string and numeric partition columns.

2. **Schema column ordering** — Reader outputs `[dataColumns, partColumns]` but `readSchema` may interleave them. Mitigated by `UnsafeProjection` reordering step.

3. **`mandatoryFields` (partition columns also in file)** — For timestamp/custom key generators, some partition columns must be read from the data file. PR2 defers this to a later PR; the common case (partition values from path) is handled.

4. **Non-vectorized reading** — PR2 uses `enableVectorizedRead = false` for simplicity. Vectorized reading can be added later for performance.

## Scope Exclusions (deferred to later PRs)

- Filter pushdown (PR3)
- MoR log file merging (PR4)
- Schema evolution / `InternalSchema` support
- Vectorized batch reading
- `mandatoryFields` for timestamp key generators
- File splitting (reading large files across multiple tasks)
