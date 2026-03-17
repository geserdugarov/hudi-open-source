# Plan: Mark PR3 as Done + Create PR4 Plan (MoR Snapshot Read)

## Task 1: Update `plans/hudi_v2/incremental_pr_plan.md` — Mark PR3 as DONE

Changes:
- Line 198: `**Status: NOT STARTED**` → `**Status: DONE**`
- Lines 229-233: Mark all verification checklist items `[ ]` → `[x]`
- Line 375: `PR 3: Filter Pushdown  ⬜ NOT STARTED` → `PR 3: Filter Pushdown  ✅ DONE`
- Line 246: `**Depends on:** PR 3 (NOT STARTED)` → `**Depends on:** PR 3 (DONE)`

---

## Task 2: Create `plans/hudi_v2/pr4.md` — MoR Snapshot Read

### Context

The DSv2 read path (PRs 1-3) currently reads only CoW tables (base Parquet files). PR4 extends it to support MoR tables by including log files during file listing and using `HoodieFileGroupReader` for merging base + log files on the executor. The key principle: for file slices with no log files (CoW, or MoR base-only), the existing fast path via `SparkColumnarFileReader` is unchanged. For file slices with log files, the reader delegates to `HoodieFileGroupReader` — the same engine-agnostic merge implementation used by DSv1's `HoodieMergeOnReadRDDV2`.

### Approach: Reuse DSv1 MoR Merge Pattern

The DSv1 path (`HoodieMergeOnReadRDDV2`) already does this:
1. `HoodieFileIndex` with `includeLogFiles = true` — includes log files in `FileSlice` objects
2. For each split with log files: creates `SparkFileFormatInternalRowReaderContext` + `HoodieFileGroupReader` via builder
3. `HoodieFileGroupReader.getClosableIterator()` returns merged `InternalRow`s

PR4 copies this pattern into a new `HoodieMorPartitionReader` class.

### New Case Class: `MorContext`

Define in `HoodieBatchScan.scala` (before the class):

```scala
case class MorContext(
  latestCommitTimestamp: String,
  tableDataSchema: HoodieSchema,      // full table Avro schema for HoodieFileGroupReader.withDataSchema
  requiredMergeSchema: HoodieSchema,  // required Avro schema for HoodieFileGroupReader.withRequestedSchema
  basePath: String,                    // table base path for rebuilding metaClient on executor
  mergeType: String,                   // e.g. "payload_combine"
  options: Map[String, String])        // table options
```

All fields are `Serializable`. Passed from `HoodieScanBuilder` → `HoodieBatchScan` → `HoodiePartitionReaderFactory` → `HoodieMorPartitionReader`.

### File Changes

#### 1. `HoodieInputPartition.scala` — Add log file info + partition path

**Path:** `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieInputPartition.scala`

```scala
case class HoodieInputPartition(
  index: Int,
  baseFilePath: String,               // "" if log-only file group
  baseFileLength: Long,               // 0 if log-only
  partitionValues: Array[AnyRef],
  logFiles: List[HoodieLogFile],      // NEW: empty for CoW / base-only
  partitionPath: String               // NEW: relative partition path
) extends InputPartition
```

New imports: `org.apache.hudi.common.model.HoodieLogFile`

#### 2. `HoodieScanBuilder.scala` — Detect table type, include log files, compute MoR context

**Path:** `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieScanBuilder.scala`

**Changes:**

a) Detect table type:
```scala
private val isMoR: Boolean =
  metaClient.getTableConfig.getTableType == HoodieTableType.MERGE_ON_READ
```

b) Change `fileIndex` to use `includeLogFiles = isMoR`:
```scala
private lazy val fileIndex = HoodieFileIndex(spark, metaClient, None, options,
  includeLogFiles = isMoR, shouldEmbedFileSlices = false)
```

c) In `build()`, update file slice → partition mapping:
- Change filter from `fs.getBaseFile.isPresent` to `fs.getBaseFile.isPresent || fs.hasLogFiles`
- Extract log files via `fs.getLogFiles.sorted(HoodieLogFile.getLogFileComparator).iterator().asScala.toList`
- Extract relative partition path from `partitionOpt.map(_.getPath).getOrElse("")`
- Pass new fields to `HoodieInputPartition`

d) For MoR, compute `MorContext`:
- `latestCommitTimestamp`: from `metaClient.getCommitsAndCompactionTimeline.filterCompletedInstants.lastInstant()`
- `tableDataSchema`: `HoodieBaseRelation.convertToHoodieSchema(tableSchema, tableName)` (full table schema as Avro)
- `requiredMergeSchema`: same conversion on the required schema (augmented with mandatory merge fields)
- `basePath`: `metaClient.getBasePath.toString`
- `mergeType`: from options or default
- `options`: existing `options` map

e) Augment required schema with mandatory merge fields for MoR:
```scala
private def augmentSchemaForMerge(schema: StructType): StructType = {
  if (!isMoR) return schema
  val recordKeyField = if (metaClient.getTableConfig.populateMetaFields()) {
    HoodieRecord.RECORD_KEY_METADATA_FIELD
  } else {
    metaClient.getTableConfig.getRecordKeyFields.get().head
  }
  val orderingFields = {
    val fields = metaClient.getTableConfig.getOrderingFields
    if (fields.isEmpty) Seq.empty else fields.asScala.toSeq
  }
  val mandatoryFields = Seq(recordKeyField) ++ orderingFields
  val missingFields = mandatoryFields.flatMap { f =>
    if (schema.fieldNames.contains(f)) None
    else tableSchema.fields.find(_.name == f)
  }
  StructType(schema.fields ++ missingFields)
}
```
Apply this when creating `requiredMergeSchema`. The `readSchema` passed to `HoodieBatchScan` remains the user's original `requiredSchema` (without augmentation).

f) Pass `morContext: Option[MorContext]` to `HoodieBatchScan` constructor.

**New imports:**
- `org.apache.hudi.common.model.{HoodieLogFile, HoodieRecord, HoodieTableType}`
- `org.apache.hudi.common.config.HoodieReaderConfig`
- `org.apache.hudi.HoodieBaseRelation.convertToHoodieSchema`
- `org.apache.hudi.common.schema.HoodieSchema`
- `org.apache.hudi.HoodieSchemaConversionUtils`
- `scala.collection.JavaConverters._`

#### 3. `HoodieBatchScan.scala` — Pass MoR context to factory

**Path:** `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieBatchScan.scala`

- Add `morContext: Option[MorContext] = None` constructor parameter
- Pass it to `HoodiePartitionReaderFactory` in `createReaderFactory()`
- Add `MorContext` case class definition before the class

#### 4. `HoodiePartitionReaderFactory.scala` — Route CoW vs MoR reader

**Path:** `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodiePartitionReaderFactory.scala`

- Add `morContext: Option[MorContext] = None` constructor parameter
- In `createReader()`: if `partition.logFiles.isEmpty` → existing `HoodiePartitionReader`; else → new `HoodieMorPartitionReader`

#### 5. `HoodieMorPartitionReader.scala` — NEW: MoR merge reader

**Path:** `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieMorPartitionReader.scala`

Core new class. Wraps `HoodieFileGroupReader` as a `PartitionReader[InternalRow]`.

**Constructor parameters:** same as `HoodiePartitionReader` + `MorContext`

**`createIterator()` logic (follows `HoodieMergeOnReadRDDV2.compute()` pattern):**
1. Get Hadoop config from broadcast
2. Create `HadoopStorageConfiguration`
3. Rebuild `HoodieTableMetaClient` on executor from `morContext.basePath` + storage conf
4. Build `TypedProperties` with merge config:
   - `MAX_MEMORY_FOR_MERGE.key()` → from `getMaxCompactionMemoryInBytes(new JobConf(hadoopConf))`
   - `HoodieReaderConfig.MERGE_TYPE.key()` → `morContext.mergeType`
   - Plus all entries from `morContext.options`
5. Build `HoodieBaseFile` option from `partition.baseFilePath` (empty string → `HOption.empty`)
6. Create `SparkFileFormatInternalRowReaderContext(broadcastReader.value, Seq.empty, Seq.empty, storageConf, metaClient.getTableConfig)`
7. Build `HoodieFileGroupReader`:
   ```
   .withReaderContext(readerContext)
   .withHoodieTableMetaClient(metaClient)
   .withLatestCommitTime(morContext.latestCommitTimestamp)
   .withLogFiles(partition.logFiles.asJava.stream())
   .withBaseFileOption(baseFileOption)
   .withPartitionPath(partition.partitionPath)
   .withProps(properties)
   .withDataSchema(morContext.tableDataSchema)
   .withRequestedSchema(morContext.requiredMergeSchema)
   .withInternalSchema(HOption.empty())
   .build()
   ```
8. Wrap `fileGroupReader.getClosableIterator` as `Iterator[InternalRow] with Closeable`
9. If merge schema differs from read schema (mandatory fields were added), apply `UnsafeProjection` to project output to `readSchema`
10. Handle partition values: append partition values to each row and project to final `readSchema` (same pattern as `HoodiePartitionReader`)

**Key reused utilities:**
| Utility | Location |
|---------|----------|
| `HoodieFileGroupReader` | `hudi-common/.../common/table/read/HoodieFileGroupReader.java` |
| `SparkFileFormatInternalRowReaderContext` | `hudi-client/hudi-spark-client/.../SparkFileFormatInternalRowReaderContext.scala` |
| `HoodieBaseRelation.convertToHoodieSchema` | `hudi-spark-datasource/hudi-spark-common/.../HoodieBaseRelation.scala` |
| `HoodieLogFile.getLogFileComparator` | `hudi-common/.../common/model/HoodieLogFile.java` |
| `FSUtils.getRelativePartitionPath` | `hudi-common/.../common/fs/FSUtils.java` |
| `HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes` | `hudi-hadoop-mr` |

#### 6. `HoodiePartitionReader.scala` — No changes needed

Existing class only uses `baseFilePath`, `baseFileLength`, `partitionValues` — all unchanged.

#### 7. `TestDSv2MorSnapshotRead.scala` — NEW test file

**Path:** `hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/spark/sql/hudi/v2/TestDSv2MorSnapshotRead.scala`

Extends `SparkClientFunctionalTestHarness`, `@Tag("functional")`.

**Test cases:**
1. `testMorReadBaseOnlyViaDataFrameApi` — MoR table, initial insert only (base files), DSv2 read matches DSv1
2. `testMorReadWithLogFilesViaDataFrameApi` — Insert + upsert (creates log files), verify merged data matches DSv1
3. `testMorReadViaSqlCatalog` — SQL path with `use.v2=true`, INSERT + UPDATE, compare results
4. `testMorWithDeletes` — Insert then delete, verify deleted records absent
5. `testMorColumnPruning` — MoR with updates, SELECT subset of columns
6. `testMorPartitionedTable` — Partitioned MoR table with updates across partitions
7. `testMorDsv1VsDsv2ResultComparison` — Comprehensive comparison: write, update, delete
8. `testMorWithMultipleLogFiles` — Multiple upserts creating multiple log files per file group

### Implementation Order

1. `HoodieInputPartition.scala` — Add fields (no dependencies)
2. `MorContext` case class in `HoodieBatchScan.scala`
3. `HoodieMorPartitionReader.scala` — New file
4. `HoodiePartitionReaderFactory.scala` — Add routing
5. `HoodieBatchScan.scala` — Pass MoR context
6. `HoodieScanBuilder.scala` — Orchestrate everything
7. `TestDSv2MorSnapshotRead.scala` — Tests
8. Update `plans/hudi_v2/incremental_pr_plan.md` — Mark PR3 done, PR4 status

### Verification

```bash
# Load environment
export $(grep -v '^#' .env | xargs)

# Build changed modules
JAVA_HOME=${JAVA_HOME_PATH} mvn clean install -DskipTests \
  -Dmaven.repo.local=${MAVEN_REPO_PATH} \
  -Djava11 -Djava.version=11 -Dspark3.5 -Dflink1.20 \
  -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true \
  -Dmaven.wagon.httpconnectionManager.ttlSeconds=25 -Dmaven.wagon.http.retryHandler.count=5 \
  -pl hudi-spark-datasource/hudi-spark-common,hudi-spark-datasource/hudi-spark -am

# Run MoR snapshot read tests
JAVA_HOME=${JAVA_HOME_PATH} mvn test -Pfunctional-tests \
  -Dmaven.repo.local=${MAVEN_REPO_PATH} \
  -Djava11 -Djava.version=11 -Dspark3.5 -Dflink1.20 \
  -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true \
  -Dmaven.wagon.httpconnectionManager.ttlSeconds=25 -Dmaven.wagon.http.retryHandler.count=5 \
  -pl hudi-spark-datasource/hudi-spark \
  -Dtest=TestDSv2MorSnapshotRead

# Run existing tests to verify no regressions
JAVA_HOME=${JAVA_HOME_PATH} mvn test -Pfunctional-tests \
  -Dmaven.repo.local=${MAVEN_REPO_PATH} \
  -Djava11 -Djava.version=11 -Dspark3.5 -Dflink1.20 \
  -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true \
  -Dmaven.wagon.httpconnectionManager.ttlSeconds=25 -Dmaven.wagon.http.retryHandler.count=5 \
  -pl hudi-spark-datasource/hudi-spark \
  -Dtest=TestDSv2CowSnapshotRead,TestDSv2CoexistenceWithDSv1,TestDSv2FilterPushdown
```

### Key Risks

1. **`HoodieTableMetaClient` on executor** — Rebuilding from `basePath` requires I/O to read `.hoodie` metadata. If slow, can switch to serializing `metaClient` directly (it implements `Serializable`).

2. **Schema conversion to `HoodieSchema`** — Must use `HoodieBaseRelation.convertToHoodieSchema(structType, tableName)` to get correct Avro record name/namespace. Incorrect conversion causes merge failures.

3. **Mandatory merge fields** — If record key or pre-combine fields are not in the merge schema, `HoodieFileGroupReader` will fail. The `augmentSchemaForMerge()` method handles this, but edge cases with virtual keys need testing.

4. **Partition values in MoR output** — `HoodieFileGroupReader` does NOT include partition columns in output. Must append partition values and project to `readSchema`, same as the CoW reader.

5. **`getMaxCompactionMemoryInBytes`** — Depends on `hudi-hadoop-mr` module. Fallback: use `HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.defaultValue()` if import unavailable.
