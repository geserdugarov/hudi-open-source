# Hudi Delete Mechanisms — Hard Deletes, Soft Deletes, Tombstones, and Partition Deletes

This document provides a comprehensive description of how Apache Hudi handles deletes: record-level hard deletes on Copy-on-Write (CoW) and Merge-on-Read (MoR) tables, data-driven deletes via the `_hoodie_is_deleted` convention, the soft-delete pattern, partition-level deletes via replace commits, and how indexes, commit metadata, compaction, and cleaning interact with all of them.

---

## 1. Overview — The Hudi Delete Model

Hudi base files are **immutable** — once written, they are never modified in place. A delete therefore always materializes as new data on storage: either a rewritten base file without the deleted rows (CoW), or a lightweight **tombstone** appended to a log file that suppresses the row at read time (MoR). Unlike formats with standalone delete files, Hudi routes deletes through the same **upsert machinery** used for updates: deletes are records with an *empty payload*, tagged to their file group by the index, and merged with existing data by handles/readers.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         HUDI DELETE FLAVORS                                  │
│                                                                              │
│  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────────┐  │
│  │  Hard delete       │  │  Data-driven       │  │  Partition delete      │  │
│  │  (by key)          │  │  delete (by field) │  │  (delete_partition)    │  │
│  │                    │  │                    │  │                        │  │
│  │  delete(keys) API  │  │  _hoodie_is_deleted│  │  replacecommit marks   │  │
│  │  or SQL DELETE     │  │  = true on an      │  │  ALL file groups of a  │  │
│  │  FROM; record is   │  │  upserted record;  │  │  partition replaced;   │  │
│  │  an empty payload  │  │  payload/merger    │  │  no per-record work    │  │
│  │                    │  │  resolves to delete│  │                        │  │
│  │  Scope: record     │  │  Scope: record     │  │  Scope: partition      │  │
│  │  Needs: index tag  │  │  Needs: index tag  │  │  Needs: file listing   │  │
│  └────────────────────┘  └────────────────────┘  └────────────────────────┘  │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │  Soft delete (user pattern, not a separate engine mechanism):          │  │
│  │  upsert the record key with all non-key fields set to null.            │  │
│  │  The row REMAINS in the table — it is an ordinary update whose         │  │
│  │  payload happens to be null-valued.                                    │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  Storage effect:   CoW  → rewrite base file without deleted rows (commit)   │
│                    MoR  → append HoodieDeleteBlock tombstone (deltacommit;  │
│                           changelog mode logs delete rows as data instead)  │
│                    Partition → replacecommit metadata only (no data files)  │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Comparison Table

| Property               | Hard delete (record-level)                                                | Data-driven delete (`_hoodie_is_deleted`)                  | Soft delete (pattern)                       | Partition delete                                                 |
|------------------------|---------------------------------------------------------------------------|------------------------------------------------------------|---------------------------------------------|------------------------------------------------------------------|
| **Operation type**     | `DELETE` / `DELETE_PREPPED`                                               | `UPSERT` / `MERGE` / streaming upsert                      | `UPSERT`                                    | `DELETE_PARTITION`                                               |
| **Input**              | `DELETE`: `HoodieKey`s; `DELETE_PREPPED`: records pre-tagged with location | Full records with boolean delete field set                 | Full records with non-key fields nulled     | List of partition paths (wildcards supported)                    |
| **Representation**     | `EmptyHoodieRecordPayload` / `HoodieEmptyRecord`                          | Payload returns `Option.empty()` on merge                  | Ordinary record with null values            | `partitionToReplaceFileIds` in replace metadata                  |
| **Timeline action**    | `commit` (CoW) / `deltacommit` (MoR)                                      | `commit` / `deltacommit`                                   | `commit` / `deltacommit`                    | `replacecommit`                                                  |
| **Index lookup**       | `DELETE`: required (`tagLocation`; untagged keys dropped); `DELETE_PREPPED`: skipped — records arrive tagged | Required (normal upsert tagging)                           | Required (normal upsert tagging)            | None — file listing per partition                                |
| **Row removed?**       | Yes (logically at once; physically on rewrite/compaction)                 | Yes                                                        | **No** — row stays, values nulled           | Yes (whole partition logically at once)                          |
| **Physical removal**   | CoW: next commit; MoR: at compaction; old slices removed by cleaner       | Same as hard delete                                        | Old slices removed by cleaner               | Replaced file groups removed by cleaner                          |
| **Write cost**         | Index lookup + merge/append                                               | Same as upsert                                             | Same as upsert                              | Very low — metadata-only commit                                  |

---

## 2. How a Delete Is Represented

### 2.1 Empty payloads and empty records (hard deletes)

A record-level hard delete is a `HoodieRecord` that carries **no data**:

```java
// hudi-common/src/main/java/org/apache/hudi/common/model/EmptyHoodieRecordPayload.java
public class EmptyHoodieRecordPayload implements HoodieRecordPayload<EmptyHoodieRecordPayload> {
  ...
  combineAndGetUpdateValue(...)  → Option.empty()   // merge result: delete
  getInsertValue(...)            → Option.empty()   // insert value: nothing
}
```

`HoodieDeleteHelper.createDeleteRecords()` converts incoming `HoodieKey`s into delete records (`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/commit/HoodieDeleteHelper.java`):

- For the AVRO record type: `new HoodieAvroRecord<>(key, new EmptyHoodieRecordPayload())`
- For other record types (e.g. Spark rows): `new HoodieEmptyRecord<>(key, recordType)` — a record whose `getData()` is `null` and whose `checkIsDelete()` returns `true` (`hudi-common/src/main/java/org/apache/hudi/common/model/HoodieEmptyRecord.java`)

Throughout the engine, an empty `Option` returned from `HoodieRecordPayload.combineAndGetUpdateValue()` or `getInsertValue()` is the universal "this record is deleted" signal.

### 2.2 The `_hoodie_is_deleted` field (data-driven deletes)

`HoodieRecord.HOODIE_IS_DELETED_FIELD = "_hoodie_is_deleted"` (`hudi-common/src/main/java/org/apache/hudi/common/model/HoodieRecord.java`) is a boolean field a user can add to their schema. When `true`, the standard payloads treat the record as a delete during merge:

```java
// hudi-common/src/main/java/org/apache/hudi/common/model/BaseAvroPayload.java
protected boolean isDeleteRecord(GenericRecord genericRecord) {
  final String isDeleteKey = HoodieRecord.HOODIE_IS_DELETED_FIELD;
  if (genericRecord.getSchema().getField(isDeleteKey) == null) {
    return false;
  }
  Object deleteMarker = genericRecord.get(isDeleteKey);
  return (deleteMarker instanceof Boolean && (boolean) deleteMarker);
}
```

Honored by `OverwriteWithLatestAvroPayload`, `DefaultHoodieRecordPayload`, `EventTimeAvroPayload`, `PartialUpdateAvroPayload` (all under `hudi-common/src/main/java/org/apache/hudi/common/model/`). This is the primary delete mechanism for streaming ingest (HoodieStreamer upsert mode, CDC pipelines) where deletes arrive interleaved with inserts/updates in the same batch. Note it only works when the configured payload/merger actually checks the field — e.g. the Kafka Connect sink wraps records in `HoodieAvroPayload`, which does not (section 10.5).

**Only effective on upsert-style operations.** Data-driven deletes require the record to be tagged and merged against its existing file group, which happens on `UPSERT`/`MERGE` paths. On plain `INSERT`/`BULK_INSERT` paths the create handle simply skips delete-marked incoming records — `BaseCreateHandle.doWrite()` (`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/BaseCreateHandle.java`) drops them and counts them in `recordsDeleted` — so an existing row in the table is **not** deleted; the marker is a no-op against stored data.

`DefaultHoodieRecordPayload` additionally supports a **custom delete field/marker** pair: `hoodie.payload.delete.field` (`DELETE_KEY`) and `hoodie.payload.delete.marker` (`DELETE_MARKER`) — any column/value combination can mark deletion.

The file-group-reader stack centralizes all three detection modes in `DeleteContext` (`hudi-common/src/main/java/org/apache/hudi/common/table/read/DeleteContext.java`):

1. the built-in `_hoodie_is_deleted` field,
2. the configured custom delete key/marker pair,
3. the `_hoodie_operation` meta field (`HoodieOperation.DELETE` / `UPDATE_BEFORE`).

### 2.3 `HoodieOperation` (changelog semantics)

`hudi-common/src/main/java/org/apache/hudi/common/model/HoodieOperation.java` encodes CDC-style row kinds, persisted in the optional `_hoodie_operation` meta field (used notably by Flink changelog mode):

```java
public enum HoodieOperation {
  INSERT("I", (byte) 0),
  UPDATE_BEFORE("-U", (byte) 1),
  UPDATE_AFTER("U", (byte) 2),
  DELETE("D", (byte) 3);
}
```

Both `DELETE` and `UPDATE_BEFORE` are treated as deletions when merging (`BufferedRecord.isDelete()`, `hudi-common/src/main/java/org/apache/hudi/common/table/read/BufferedRecord.java`).

### 2.4 `DeleteRecord` — the tombstone unit (MoR)

```java
// hudi-common/src/main/java/org/apache/hudi/common/model/DeleteRecord.java
public class DeleteRecord implements Serializable {
  private final HoodieKey hoodieKey;         // record key + partition path
  private final Comparable<?> orderingValue; // event-time/precombine value of the delete
}
```

The `orderingValue` matters: it lets a late-arriving record with a **newer event time outlive an older delete** under event-time ordering (see section 6.3).

### 2.5 Soft deletes (user pattern)

Hudi has no dedicated soft-delete machinery. The documented user pattern is: **upsert the record keeping its key fields and setting every non-key field to null**. The row remains queryable (key present, values null), the index still maps the key to its file group, and a later upsert can "undelete" it by writing real values again. From the engine's perspective this is an ordinary update. Contrast with `_hoodie_is_deleted = true`, which is a **hard** delete expressed through data.

---

## 3. Record-Key Lookup and Index Interactions

A `DELETE` only carries keys, so Hudi must locate the file group currently holding each key. This is the same index lookup (**tagging**) used for upserts. `DELETE_PREPPED` skips the lookup entirely: its input records already carry their `HoodieRecordLocation` (e.g. Spark SQL optimized writes read the location from Hudi meta fields), and the `*DeletePreppedCommitActionExecutor`s feed them straight into the write machinery — the flow below applies only to non-prepped `DELETE`.

### 3.1 Tagging flow

```
client.delete(keys, instantTime)
        │
        ▼
HoodieDeleteHelper.execute()              [hudi-client-common table/action/commit]
  │
  ├─► 1. Deduplicate keys (if hoodie.combine.before.delete, default true):
  │       global index    → distinct by record key only
  │       non-global index→ distinct by (record key, partition path)
  │
  ├─► 2. createDeleteRecords(): HoodieKey → record with empty payload
  │
  ├─► 3. table.getIndex().tagLocation(records, context, table)
  │       └─► annotates each record with a HoodieRecordLocation
  │           (lookup indexes: only if the key exists;
  │            bucket indexes: the key's bucket, existence not checked)
  │
  ├─► 4. filter(HoodieRecord::isCurrentLocationKnown)
  │       └─► deletes with NO TAGGED LOCATION are silently DROPPED
  │           (nothing to delete — no tombstone is written).
  │           NOTE: with bucket indexes this does NOT filter
  │           non-existent keys — see bucket-index exception below
  │
  └─► 5. Route tagged records into the upsert write machinery
          (partition into buckets → merge/append handles → commit)
```

Source: `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/commit/HoodieDeleteHelper.java` (Spark); `JavaDeleteHelper` is the Java equivalent under `hudi-client/hudi-java-client/...`. The commit executors are `SparkDeleteCommitActionExecutor` / `JavaDeleteCommitActionExecutor` (+ `SparkDeletePreppedCommitActionExecutor` / `JavaDeletePreppedCommitActionExecutor` for pre-tagged records). Flink has **no key-based delete executor** — `HoodieFlinkCopyOnWriteTable.delete()` throws `HoodieNotSupportedException`; Flink record deletes arrive pre-tagged through the streaming pipeline and are committed by `FlinkDeletePreppedCommitActionExecutor` (`hudi-client/hudi-flink-client/src/main/java/org/apache/hudi/table/action/commit/FlinkDeletePreppedCommitActionExecutor.java`, see section 10.4).

### 3.2 Global vs non-global indexes

`HoodieIndex.isGlobal()` (`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/index/HoodieIndex.java`): an index is *global* if the key→fileId mapping does not depend on partition path.

| Index type (`hoodie.index.type`) | Global | Delete needs correct partition path? | Lookup mechanism                       |
|----------------------------------|--------|--------------------------------------|----------------------------------------|
| `BLOOM`                          | No     | **Yes** — wrong partition = no-op    | Bloom filters in base file footers     |
| `GLOBAL_BLOOM`                   | Yes    | No — derived from index              | Bloom filters across all partitions    |
| `SIMPLE`                         | No     | **Yes**                              | Join against base file keys            |
| `GLOBAL_SIMPLE`                  | Yes    | No                                   | Join across all partitions             |
| `BUCKET`                         | No     | **Yes**                              | **No lookup** — hash(key) → bucket     |
| `RECORD_LEVEL_INDEX`             | No     | **Yes**                              | Metadata table `record_index`          |
| `GLOBAL_RECORD_LEVEL_INDEX`      | Yes    | No                                   | Metadata table `record_index`          |

Config: `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieIndexConfig.java`.

Notable per-index delete behaviors:

- **Global delete key generators.** For delete-only datasets against globally indexed tables, `GlobalDeleteKeyGenerator` (`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/keygen/GlobalDeleteKeyGenerator.java`) and `GlobalAvroDeleteKeyGenerator` (`hudi-client/hudi-client-common/.../keygen/GlobalAvroDeleteKeyGenerator.java`) produce an **empty partition path** — the global index resolves the real partition during tagging, so callers need only supply record keys.
- **Bucket index needs no lookup — and does not check key existence.** `HoodieBucketIndex.tagLocation()` (`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/index/bucket/HoodieBucketIndex.java`) computes the file group deterministically from the key hash — deletes go straight to their bucket. Because tagging returns the *bucket's* location rather than proving the key exists (`SimpleBucketIndexLocationFunction.apply()` in `.../bucket/HoodieSimpleBucketIndex.java` returns the file group mapped to `hash(key) % numBuckets`; `ConsistentBucketIndexLocationFunction.apply()` in `.../bucket/HoodieConsistentBucketIndex.java` always resolves a hashing node), the `isCurrentLocationKnown` filter in the tagging flow does **not** drop deletes for keys that were never written: a tombstone is still emitted into the bucket's file group and becomes a harmless no-op at merge time. Only a bucket with no file group yet (simple bucket index) yields an untagged — and therefore dropped — delete.
- **Bloom filters are never updated by deletes.** Bloom filters live in base file footers (`isImplicitWithStorage() == true`) and are only produced when base files are (re)written. A tombstoned key may keep "hitting" the old bloom filter; that is a benign false positive resolved by the merge.

### 3.3 Metadata table / Record Level Index maintenance

When a delete commits, the metadata table is updated via `HoodieBackedTableMetadataWriter.update(commitMetadata, instantTime)` (`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/metadata/HoodieBackedTableMetadataWriter.java`):

- **`record_index` partition:** each deleted key produces an RLI delete via `HoodieMetadataPayload.createRecordIndexDelete(...)` (`hudi-common/src/main/java/org/apache/hudi/metadata/HoodieMetadataPayload.java`) — itself an empty-payload record in the metadata table, removing the key→location mapping.
- **`DELETE_PARTITION`:** `getRecordIndexAdditionalUpserts(...)` loads *all* record keys of the replaced file groups and emits RLI deletes for every one of them (same for unmatched keys under `INSERT_OVERWRITE`/`INSERT_OVERWRITE_TABLE`).
- **`files` / `column_stats` partitions:** updated from the commit's write stats — new log files (containing delete blocks) are registered, and files removed by replace/clean operations are marked deleted (`HoodieTableMetadataUtil.convertMetadataToFilesPartitionRecords`, `hudi-common/src/main/java/org/apache/hudi/metadata/HoodieTableMetadataUtil.java`).

---

## 4. Copy-on-Write Delete Handling

On CoW, a delete triggers a **rewrite of the affected base file**; the resulting commit replaces the old file slice.

```
BEFORE:                                  AFTER (commit C2):
┌──────────────────────┐                 ┌──────────────────────┐
│ fg-1_C1.parquet      │                 │ fg-1_C2.parquet      │  (new slice, same file group)
│  row A  ◄── DELETE   │   ══════════►   │  row B               │
│  row B               │                 │  row C               │
│  row C               │                 └──────────────────────┘
└──────────────────────┘                 old slice fg-1_C1 retained until cleaned

Timeline: commit C2 (action = "commit").  No tombstones produced.
```

### Write flow

```
SparkRDDWriteClient.delete(keys, t)                  [hudi-spark-client client/]
  └─► HoodieDeleteHelper.execute()                   tag + filter (section 3.1)
        └─► merge partition: existing file group → HoodieWriteMergeHandle

HoodieWriteMergeHandle                               [hudi-client-common io/HoodieWriteMergeHandle.java]
  │
  ├─► for each incoming record, merge against the matching base file record
  │     (record merger / payload combineAndGetUpdateValue)
  │
  ├─► writeRecord(key, combineRecord, schema, props, isDelete):
  │     if (!isDelete && !combineRecord.isDelete(deleteContext, props)):
  │         writeToFile(...)            ── survivor copied to new base file
  │         recordsWritten++
  │     else:
  │         recordsDeleted++            ── record is DROPPED (not written)
  │         (secondary index tracker notified with isDelete=true)
  │
  └─► close(): HoodieWriteStat { numDeletes = recordsDeleted, ... }
```

`isDeleteRecord()` in the merge handle also treats `HoodieOperation.DELETE` / `UPDATE_BEFORE` operations as deletes. The deleted rows simply never make it into the new base file; the deletion becomes effective the moment commit `C2` completes on the timeline.

- **Write cost:** high — entire base file(s) rewritten even for one deleted row
- **Read cost:** zero — readers of the latest slice see clean data immediately
- **Old data:** previous slice remains on storage (time travel works) until the cleaner removes it

---

## 5. Merge-on-Read Delete Handling — Log Blocks and Tombstones

On MoR, a delete appends a **delete block** (tombstone block) to the log file of the target file slice — or, in changelog mode, a data block carrying the delete as a full row marked `op=D` (see "Changelog mode" below). Base files are untouched either way.

```
BEFORE:                                  AFTER (deltacommit DC2):
┌──────────────────────┐                 ┌──────────────────────┐
│ fg-1_C1.parquet      │                 │ fg-1_C1.parquet      │  (unchanged)
│  row A  ◄── DELETE   │   ══════════►   │  row A               │
│  row B               │                 │  row B               │
│  row C               │                 └──────────────────────┘
└──────────────────────┘                 ┌──────────────────────┐
                                         │ .fg-1_C1.log.1       │
                                         │  ┌────────────────┐  │
                                         │  │ DELETE_BLOCK   │  │  tombstone:
                                         │  │  (key A, ord.) │  │  DeleteRecord[]
                                         │  └────────────────┘  │
                                         └──────────────────────┘

Timeline: deltacommit DC2.  Delete applied at read time / compaction.
```

### 5.1 Write flow

```
HoodieAppendHandle.bufferRecord()                    [hudi-client-common io/HoodieAppendHandle.java]
  │
  ├─► for each record routed to this file slice:
  │     if config.allowOperationMetadataField()
  │        OR record is NOT a delete (empty payload / delete field / operation):
  │       bufferInsertAndUpdate(...)  → record goes into a DATA block
  │         (when the flag is on, populateMetadataFields() persists the
  │          record's _hoodie_operation — a delete stays a full data row
  │          marked op=D instead of becoming a tombstone)
  │     else:
  │       bufferDelete(record) → addDeletedKey(...):
  │         recordsDeleted++
  │         orderingVal = record.getOrderingValueAsJava(...)
  │         recordsToDeleteWithPositions.add(
  │             Pair.of(DeleteRecord.create(key, orderingVal), position))
  │
  └─► flush (appendDataAndDeleteBlocks):
        blocks += new HoodieDataBlock(...)                 if any data records
        blocks += new HoodieDeleteBlock(                   if any tombstones
                    recordsToDeleteWithPositions, headers)
        HoodieLogFormat.Writer.appendBlocks(blocks)
```

A single deltacommit's log file can thus contain a data block (updates/inserts) followed by a delete block (tombstones) for the same file group.

**Changelog mode — deletes as data rows, not tombstones.** When `hoodie.allow.operation.metadata.field` is enabled (`HoodieWriteConfig.ALLOW_OPERATION_METADATA_FIELD`, `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieWriteConfig.java`), the append handle never emits `HoodieDeleteBlock`s: delete records are buffered into ordinary **data blocks** as full rows with the `_hoodie_operation` meta field set to `D`/`-U`, so every change of a record is persisted to the log without merge. Flink wires this flag from `changelog.enabled` (`FlinkWriteClients`, `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/util/FlinkWriteClients.java`: `.withAllowOperationMetadataField(conf.get(FlinkOptions.CHANGELOG_ENABLED))`) — that is how MoR retains the full `+I/-U/+U/-D` changelog for streaming reads. Such deletes are recognized at read time through the operation field (sections 2.3 and 6) rather than through tombstone blocks.

### 5.2 `HoodieDeleteBlock` format

`hudi-common/src/main/java/org/apache/hudi/common/table/log/block/HoodieDeleteBlock.java`; block type registry in `HoodieLogBlock.HoodieLogBlockType` (`.../table/log/block/HoodieLogBlock.java`):

```java
public enum HoodieLogBlockType {
  COMMAND_BLOCK(":command", ...),   // e.g. rollback markers
  DELETE_BLOCK(":delete", ...),     // tombstones — keys to delete, NO data
  CORRUPT_BLOCK(":corrupted", ...),
  AVRO_DATA_BLOCK("avro", ...),
  HFILE_DATA_BLOCK("hfile", ...),
  PARQUET_DATA_BLOCK("parquet", ...),
  CDC_DATA_BLOCK("cdc", ...);
}
```

```
Log file (HoodieLogFormat):
┌─────────────────────────────────────────────────────────────┐
│ magic | block size | log format version | block type        │
│ header map: INSTANT_TIME, TARGET_INSTANT_TIME, SCHEMA, ...  │
│ content:                                                    │
│   DELETE_BLOCK version 3 → Avro HoodieDeleteRecordList:     │
│     [ { recordKey, partitionPath, orderingVal }, ... ]      │
│   (version 2: binary-serialized DeleteRecord[]; version 1:  │
│    legacy HoodieKey[] without ordering values)              │
│ footer | block length                                       │
└─────────────────────────────────────────────────────────────┘
```

Serialization is versioned (`serializeV2`/`serializeV3`, with `deserialize()` reading all versions). Version 3 (Avro) persists the **ordering value** per tombstone, so event-time merge semantics survive in the log.

- **Write cost:** very low — only keys + ordering values appended
- **Read cost:** moderate — tombstones must be loaded and merged on every snapshot read until compaction
- **Physical removal:** at compaction (section 9.1)

---

## 6. Delete Application During Reads — Unified Flow

Snapshot reads of MoR file slices (and CoW merges, and compaction — all share this stack) apply tombstones through the **file group reader**.

### 6.1 Flow

```
HoodieFileGroupReader                                [hudi-common table/read/HoodieFileGroupReader.java]
  │
  ├─► 1. Scan log files of the file slice (forward order, oldest → newest
  │       valid blocks — LogScanningRecordBufferLoader builds the log reader
  │       with withReverseReader(false) [table/read/buffer/LogScanningRecordBufferLoader.java]):
  │       │
  │       ├─► DATA block   → buffer each record:
  │       │     KeyBasedFileGroupRecordBuffer.processNextDataRecord(...)
  │       │       records[key] = bufferedRecordMerger.deltaMerge(new, existing)
  │       │
  │       └─► DELETE block → buffer each tombstone:
  │             processDeleteBlock(deleteBlock)       [table/read/buffer/KeyBasedFileGroupRecordBuffer.java]
  │               for each DeleteRecord:
  │                 processNextDeletedRecord(deleteRecord, recordKey):
  │                   existing = records.get(recordKey)
  │                   opt = bufferedRecordMerger.deltaMerge(deleteRecord, existing)
  │                   if opt present:                  delete wins →
  │                     records[recordKey] = BufferedRecords.fromDeleteRecord(...)
  │                   (else: existing record outranks the delete — tombstone ignored)
  │
  ├─► 2. Iterate base file, merging against the buffer (doHasNext):
  │       hasNextBaseRecord(baseRecord, logRecordInfo):
  │         mergeResult = bufferedRecordMerger.finalMerge(baseRecordInfo, logRecordInfo)
  │         if mergeResult.isDelete():  SKIP row (not emitted)       ◄── tombstone applied
  │         else:                       emit merged row
  │
  └─► 3. Emit remaining buffered log-only records,
          skipping those flagged as deletes (unless emitDelete, e.g. CDC reads)
```

`PositionBasedFileGroupRecordBuffer` (same package) keys the buffer by **base-file row position** instead of record key when positions were recorded in block headers, falling back to key-based merging when unavailable.

### 6.2 Merge modes

`RecordMergeMode` (`hudi-common/src/main/java/org/apache/hudi/common/config/RecordMergeMode.java`), implemented by mergers built in `BufferedRecordMergerFactory` (`hudi-common/src/main/java/org/apache/hudi/common/table/read/BufferedRecordMergerFactory.java`):

| Mode                   | Winner between two versions of a key       | Delete behavior                                                          |
|------------------------|---------------------------------------------|---------------------------------------------------------------------------|
| `COMMIT_TIME_ORDERING` | Later commit wins                           | A delete from a later commit always suppresses the record                 |
| `EVENT_TIME_ORDERING`  | Larger ordering value (precombine field)    | Delete wins only if its ordering value is not lower than the record's     |
| `CUSTOM`               | User `HoodieRecordMerger` / payload         | Merge result of `Option.empty()` ⇒ delete                                 |

### 6.3 Ordering semantics for deletes

A tombstone carries an ordering value (section 2.4). The decision in `deltaMergeDeleteRecord(...)` (`BufferedRecordMergerFactory`):

```
existing record (ordering=5)   vs   DeleteRecord (ordering=3):
  EVENT_TIME_ORDERING:  existing wins → tombstone DISCARDED (late delete loses)

existing record (ordering=3)   vs   DeleteRecord (ordering=5):
  delete wins → record suppressed

DeleteRecord with DEFAULT ordering value (0):
  "commit-time-ordering delete" (BufferedRecord.isCommitTimeOrderingDelete) —
  treated as later-commit-wins; suppresses the record regardless of event time
```

The same rules apply in payload-based merging: `DefaultHoodieRecordPayload.preCombine()` keeps the highest ordering value, and `HoodieAvroRecordMerger` (`hudi-common/src/main/java/org/apache/hudi/common/model/HoodieAvroRecordMerger.java`) interprets an empty merge result as a deletion.

---

## 7. Partition Deletes — `DELETE_PARTITION` and Replace Commits

Deleting whole partitions skips per-record work entirely: a **replacecommit** marks every file group of the target partitions as *replaced* (with no replacement files), making them instantly invisible.

```
client.deletePartitions(["2024/01/01", "2024/01/02"], t)
        │
        ▼
SparkDeletePartitionCommitActionExecutor             [hudi-spark-client table/action/commit/]
  │       (extends SparkInsertOverwriteCommitActionExecutor)
  │
  ├─► DeletePartitionUtils.checkForPendingTableServiceActions(table, partitions)
  │     [hudi-client-common client/utils/DeletePartitionUtils.java]
  │     └─► throws HoodieDeletePartitionPendingTableServiceException if any
  │         pending compaction/clustering touches these partitions
  │
  ├─► getAllExistingFileIds(partition) for each target partition
  │
  ├─► build Map<String, List<String>> partitionToReplaceFileIds
  │
  └─► commit REPLACE_COMMIT_ACTION ("replacecommit"):
        HoodieReplaceCommitMetadata { partitionToReplaceFileIds: {...} }
        [hudi-common common/model/HoodieReplaceCommitMetadata.java]
        (failures wrapped in HoodieDeletePartitionException)

READ SIDE (immediately after commit):
  HoodieTableFileSystemView                          [hudi-common common/table/view/]
    fgIdToReplaceInstants: {fileGroupId → replaceInstant}
    └─► getLatestFileSlices() filters out replaced file groups
        → partition data invisible to snapshot reads at once
        → physical files remain until the CLEANER removes them (section 9.2)
```

Flink has a matching executor (`FlinkDeletePartitionCommitActionExecutor`, `hudi-client/hudi-flink-client/src/main/java/org/apache/hudi/table/action/commit/FlinkDeletePartitionCommitActionExecutor.java`). The Java client does **not** support partition deletes: `HoodieJavaCopyOnWriteTable.deletePartitions()` throws `HoodieNotSupportedException` (`hudi-client/hudi-java-client/src/main/java/org/apache/hudi/table/HoodieJavaCopyOnWriteTable.java`).

### Partition TTL (automatic partition deletes)

Hudi can expire old partitions automatically (`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieTTLConfig.java`):

| Config                                          | Default        | Purpose                                            |
|--------------------------------------------------|----------------|----------------------------------------------------|
| `hoodie.partition.ttl.inline`                    | `false`        | Run TTL management after each commit               |
| `hoodie.partition.ttl.management.strategy.type`  | `KEEP_BY_TIME` | Strategy (`KeepByTimeStrategy` or custom class)    |
| `hoodie.partition.ttl.strategy.days.retain`      | `-1`           | Partitions older than N days are expired           |

`SparkPartitionTTLActionExecutor` (`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/commit/SparkPartitionTTLActionExecutor.java`) resolves expired partitions via the `PartitionTTLStrategy` (`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/ttl/strategy/`) and delegates to `SparkDeletePartitionCommitActionExecutor` — i.e. TTL **is** a `delete_partition` issued automatically.

---

## 8. Commit Metadata and Timeline

### Operation types and timeline actions

`WriteOperationType` (`hudi-common/src/main/java/org/apache/hudi/common/model/WriteOperationType.java`): `DELETE("delete")`, `DELETE_PREPPED("delete_prepped")`, `DELETE_PARTITION("delete_partition")`; helper `WriteOperationType.isDelete()` covers the first two.

| Delete flavor              | CoW timeline action | MoR timeline action | Metadata class                                       |
|----------------------------|---------------------|---------------------|------------------------------------------------------|
| Record-level delete        | `commit`            | `deltacommit`       | `HoodieCommitMetadata`                               |
| Data-driven delete (upsert)| `commit`            | `deltacommit`       | `HoodieCommitMetadata`                               |
| Partition delete           | `replacecommit`     | `replacecommit`     | `HoodieReplaceCommitMetadata` (+ replace file IDs)   |

Action constants: `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/HoodieTimeline.java`.

### Delete accounting

Every written file slice reports a `HoodieWriteStat` (`hudi-common/src/main/java/org/apache/hudi/common/model/HoodieWriteStat.java`, extending `HoodieReadStats` which holds `numDeletes` — `hudi-common/src/main/java/org/apache/hudi/common/table/read/HoodieReadStats.java`). The commit metadata records the operation type and aggregates deletions:

```java
// hudi-common/src/main/java/org/apache/hudi/common/model/HoodieCommitMetadata.java
public long getTotalRecordsDeleted() {
  // sum of stat.getNumDeletes() across all partitions/files
}
```

So a completed commit file on the timeline reports, per file group, how many delete records the write processed — i.e. rows omitted from the rewritten base file or tombstoned in the log. Note this is not always the number of rows actually removed from the table: delete-marked records skipped on `INSERT`/`BULK_INSERT` paths (section 2.2) are also counted in `recordsDeleted` even though no stored row existed to delete. These stats also drive metadata-table updates (section 3.3) and incremental/CDC consumers.

---

## 9. Compaction, Cleaning, and Retention Interactions

A Hudi delete is **logical first, physical later**. Three services finish the job:

```
Time ─────────────────────────────────────────────────────────────────►

1. DELETE (MoR)            tombstone in log file          row invisible to
   deltacommit DC2         (data still in base file)      snapshot reads

2. COMPACTION              merges base + log via          new base file WITHOUT
   commit C3               HoodieFileGroupReader          the deleted rows;
                           (tombstones applied)           old slice still on disk

3. CLEANER                 retention policy expires       old base/log files
   clean action            slice {C1 base, DC2 log}       PHYSICALLY DELETED

4. ARCHIVAL                old timeline instants moved    history beyond retained
                           to archived timeline           commits no longer queryable
```

### 9.1 Compaction physically removes tombstoned rows

Compaction reuses the read-side merge: `FileGroupReaderBasedMergeHandle.doMerge()` (`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/FileGroupReaderBasedMergeHandle.java`) iterates a `HoodieFileGroupReader` over the file slice — deletes already applied — and writes only the surviving records into the new base file (`readStats.getNumDeletes()` is recorded). Orchestrated by `HoodieCompactor` (`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/HoodieCompactor.java`). After compaction, both the deleted rows and the tombstones are gone from the **latest** slice; the pre-compaction slice lingers until cleaned.

### 9.2 Cleaning — when deleted data actually leaves storage

`CleanPlanner` (`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/clean/CleanPlanner.java`) plans physical deletion under `HoodieCleanConfig` (`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieCleanConfig.java`):

| Policy (`hoodie.clean.policy`)                            | Retained                       | Default knob                                  |
|-----------------------------------------------------------|--------------------------------|-----------------------------------------------|
| `KEEP_LATEST_COMMITS`                                      | Slices needed by last N commits| `hoodie.clean.commits.retained` = 10          |
| `KEEP_LATEST_FILE_VERSIONS`                                | Last N versions per file group | `hoodie.clean.fileversions.retained` = 3      |
| `KEEP_LATEST_BY_HOURS`                                     | Slices younger than N hours    | `hoodie.clean.hours.retained` = 24            |

Delete-relevant cleaning behavior:

- **Old file slices** containing pre-delete data are removed once they fall out of retention (`getDeletePaths()` → per-policy selection → `CleanActionExecutor` performs the deletes).
- **Replaced file groups** from `delete_partition` / insert-overwrite are picked up by `getReplacedFilesEligibleToClean(...)`, which queries the file system view's replaced-group tracking — this is the moment partition-delete data physically disappears.
- **Pending compaction protection:** slices that are inputs to pending compaction/log-compaction are never cleaned (`isFileSliceNeededForPendingMajorOrMinorCompaction`), since compaction still needs the base + log files (including delete blocks).
- **Savepoint protection:** savepointed file slices are excluded from cleaning, preserving point-in-time data (including "deleted" rows) indefinitely.

### 9.3 Consequence: deleted data remains accessible until cleaned

Because old slices and old timeline instants survive until clean/archive:

- **Time-travel queries** (`as.of.instant` before the delete) still return deleted rows — slice selection is timeline-based (`IncrementalQueryAnalyzer`, `hudi-common/src/main/java/org/apache/hudi/common/table/read/IncrementalQueryAnalyzer.java`).
- **Incremental/CDC queries** spanning the delete commit observe the deletion (and with CDC enabled, before-images).
- For **compliance deletes (e.g. GDPR)**, data is only truly gone after: delete commit → (MoR) compaction → clean of all slices containing the data → archival/expiry of any savepoints referencing them.

---

## 10. Engine-Specific Delete APIs

### 10.1 Write client (engine-agnostic surface)

`BaseHoodieWriteClient` (`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/BaseHoodieWriteClient.java`) declares the record-delete pair (fully functional on Spark and Java; on Flink they exist but are not the supported path — see below):

```java
O delete(K keys, String instantTime);                  // keys → hard delete
O deletePrepped(I preppedRecords, String instantTime); // pre-tagged delete records
```

`deletePartitions(List<String> partitions, String instantTime)` is **not** part of the base contract — it is defined directly on the Spark and Flink clients only.

| Engine | Client                                                                              | Signatures                                                                  |
|--------|--------------------------------------------------------------------------------------|------------------------------------------------------------------------------|
| Spark  | `hudi-client/hudi-spark-client/.../client/SparkRDDWriteClient.java`                  | `delete(JavaRDD<HoodieKey>, t)`, `deletePrepped(JavaRDD<HoodieRecord>, t)`, `deletePartitions(List<String>, t)` |
| Flink  | `hudi-client/hudi-flink-client/.../client/HoodieFlinkWriteClient.java`               | `deletePartitions(List<String>, t)` only. The declared `delete`/`deletePrepped` delegate to `HoodieFlinkCopyOnWriteTable` methods that throw `HoodieNotSupportedException` — record deletes flow through the streaming pipeline instead (section 10.4) |
| Java   | `hudi-client/hudi-java-client/.../client/HoodieJavaWriteClient.java`                 | `delete(List<HoodieKey>, t)`, `deletePrepped(...)` — no partition deletes (`HoodieJavaCopyOnWriteTable.deletePartitions()` throws `HoodieNotSupportedException`) |

Example usage: `hudi-examples/hudi-examples-java/src/main/java/org/apache/hudi/examples/java/HoodieJavaWriteClientExample.java`.

### 10.2 Spark DataSource

Options in `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/DataSourceOptions.scala`; routing in `HoodieSparkSqlWriter` (`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieSparkSqlWriter.scala`).

```scala
// Hard delete: write a DataFrame of keys with operation=delete
df.select("uuid", "partitionpath", "ts").write.format("hudi")
  .option("hoodie.datasource.write.operation", "delete")     // DELETE_OPERATION_OPT_VAL
  .mode(Append).save(tablePath)
// HoodieSparkSqlWriter extracts HoodieKeys via the key generator
// and calls client.delete(...) — non-key columns are ignored.

// Partition delete: empty DataFrame + partition list (wildcards via *)
spark.emptyDataFrame.write.format("hudi")
  .option("hoodie.datasource.write.operation", "delete_partition")
  .option("hoodie.datasource.write.partitions.to.delete", "2024/01/*")
  .mode(Append).save(tablePath)
// If partitions.to.delete is absent, partitions are derived from the
// DataFrame rows' partition paths.
```

Quickstart demos: `hudi-examples/hudi-examples-spark/src/main/java/org/apache/hudi/examples/quickstart/HoodieSparkQuickstart.java` (`delete(...)`, `deleteByPartition(...)`).

### 10.3 Spark SQL

- **`DELETE FROM t WHERE ...`** — `DeleteHoodieTableCommand` (`hudi-spark-datasource/hudi-spark3-common/src/main/scala/org/apache/spark/sql/hudi/command/DeleteHoodieTableCommand.scala`) filters the table by the condition, prunes to key/condition columns, and writes through `HoodieSparkSqlWriter` with the delete operation. With `hoodie.spark.sql.optimized.writes.enable=true` the plan reads Hudi meta fields and takes the prepped path (`DELETE_PREPPED`), skipping the index lookup.
- **`MERGE INTO ... WHEN MATCHED [AND cond] THEN DELETE`** — `MergeIntoHoodieTableCommand` serializes the delete condition into `ExpressionPayload` (`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/command/payload/ExpressionPayload.scala`, `PAYLOAD_DELETE_CONDITION`); matched records whose condition evaluates true resolve to a deletion at merge time.
- **`ALTER TABLE ... DROP PARTITION` / `CALL drop_partition(...)`** — partition deletes from SQL (`hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/command/procedures/DropPartitionProcedure.scala`).

### 10.4 Flink

- **Changelog ingestion:** rows with `RowKind.DELETE` (and `UPDATE_BEFORE` in full changelog mode) are converted into Hudi records by `RecordConverter` (`hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/sink/transform/RecordConverter.java`), which maps the row kind directly: `HoodieOperation.fromValue(dataRow.getRowKind().toByteValue())`. Supported changelog modes are declared in `ChangelogModes` (`hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/util/ChangelogModes.java`).
- **How Flink record deletes are committed:** there is no key-based `delete()` path — `HoodieFlinkWriteClient.delete()`/`deletePrepped()` delegate to `HoodieFlinkCopyOnWriteTable` methods that throw `HoodieNotSupportedException`. Instead, delete-flagged records ride the streaming **upsert pipeline**: the sink tags/buckets them and flushes per-bucket batches through the pipeline write methods, where the write handles recognize deletes via `HoodieOperation`/payload (sections 4–5). The pipeline-internal overload `HoodieFlinkCopyOnWriteTable.deletePrepped(context, writeHandle, bucketInfo, instantTime, preppedRecords)` (`hudi-client/hudi-flink-client/src/main/java/org/apache/hudi/table/HoodieFlinkCopyOnWriteTable.java`) is backed by `FlinkDeletePreppedCommitActionExecutor`. Partition deletes via `deletePartitions()` are supported (section 7).
- **`changelog.enabled`** (`FlinkOptions`, `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/configuration/FlinkOptions.java`): when true, MoR retains intermediate changes (including `-U`/`-D` rows) so downstream consumption sees the full changelog — the flag enables `hoodie.allow.operation.metadata.field`, so deletes land in log files as data rows marked `op=D` instead of tombstone blocks (section 5.1); when false, deletes collapse into upsert semantics and are written as `HoodieDeleteBlock` tombstones.
- **Flink SQL `DELETE FROM`** (batch) and retraction streams both reduce to the above mechanisms; `EventTimeAvroPayload` (`hudi-common/src/main/java/org/apache/hudi/common/model/EventTimeAvroPayload.java`) is the Flink-default payload honoring `_hoodie_is_deleted` with event-time ordering.

### 10.5 HoodieStreamer / Kafka Connect

- **HoodieStreamer** has explicit delete operations: when run with `--op DELETE` or `--op DELETE_PARTITION`, `StreamSync` (`hudi-utilities/src/main/java/org/apache/hudi/utilities/streamer/StreamSync.java`) maps the incoming records to keys / distinct partition paths and calls `writeClient.delete(keys, t)` / `writeClient.deletePartitions(partitions, t)` directly. In the default upsert ingestion mode, deletes interleaved in the source stream use the **`_hoodie_is_deleted` convention** (section 2.2) instead.
- **Kafka Connect** has no delete support in its write path. The sink wraps every record in `HoodieAvroPayload` (`AbstractConnectWriter`, `hudi-kafka-connect/src/main/java/org/apache/hudi/connect/writers/AbstractConnectWriter.java`) and flushes via the Java client's prepped upsert/bulk-insert paths (`BufferedConnectWriter`, `.../connect/writers/BufferedConnectWriter.java`). `HoodieAvroPayload` (`hudi-common/src/main/java/org/apache/hudi/common/model/HoodieAvroPayload.java`) does not extend `BaseAvroPayload` and has no delete-marker handling — its `getInsertValue()` only returns empty for empty record bytes — so `_hoodie_is_deleted` in source records is **not** honored when writing through the connector.

### 10.6 API summary

| Surface          | Record delete                                  | Partition delete                                  |
|------------------|------------------------------------------------|---------------------------------------------------|
| Write client     | `delete(keys)` / `deletePrepped(records)`      | `deletePartitions(partitions)` (Spark/Flink only) |
| Spark DataSource | `operation=delete` + key columns               | `operation=delete_partition` + partition list     |
| Spark SQL        | `DELETE FROM`, `MERGE ... THEN DELETE`         | `ALTER TABLE DROP PARTITION` / `call drop_partition` |
| Flink            | `RowKind.DELETE` rows, `_hoodie_is_deleted`    | `deletePartitions` via write client               |
| HoodieStreamer   | `--op DELETE`, or `_hoodie_is_deleted` in source data | `--op DELETE_PARTITION`                     |
| Kafka Connect    | — (no delete support; see section 10.5)        | —                                                 |
| Automatic        | —                                              | Partition TTL (`hoodie.partition.ttl.*`)          |

---

## 11. Key Classes Reference

| Area                       | Class                                          | Module               | Role                                                                  |
|----------------------------|------------------------------------------------|----------------------|------------------------------------------------------------------------|
| **Delete representation**  | `EmptyHoodieRecordPayload`                     | hudi-common          | Hard-delete payload — merge/insert value is `Option.empty()`           |
|                            | `HoodieEmptyRecord`                            | hudi-common          | Engine-typed delete record (`getData() == null`)                       |
|                            | `DeleteRecord`                                 | hudi-common          | Tombstone unit: `HoodieKey` + ordering value                           |
|                            | `HoodieOperation`                              | hudi-common          | `I` / `-U` / `U` / `D` changelog operations (`_hoodie_operation`)      |
|                            | `BaseAvroPayload`                              | hudi-common          | `isDeleteRecord()` — `_hoodie_is_deleted` detection                    |
|                            | `DefaultHoodieRecordPayload`                   | hudi-common          | Custom `DELETE_KEY` / `DELETE_MARKER` support                          |
|                            | `DeleteContext`                                | hudi-common          | Unified delete detection for the file group reader                     |
| **Write path**             | `BaseHoodieWriteClient`                        | hudi-client-common   | `delete()` / `deletePrepped()` contract (partition deletes: Spark/Flink clients only) |
|                            | `HoodieDeleteHelper` (`BaseDeleteHelper`)      | hudi-client-common   | Key dedup → delete records → tag → filter → upsert machinery           |
|                            | `SparkDeleteCommitActionExecutor` (+Java; Flink: prepped-only) | per-engine client | DELETE commit action (Flink: `FlinkDeletePreppedCommitActionExecutor`) |
|                            | `HoodieWriteMergeHandle`                       | hudi-client-common   | CoW merge — drops deleted records when rewriting base files            |
|                            | `HoodieAppendHandle`                           | hudi-client-common   | MoR append — accumulates tombstones, flushes delete blocks             |
| **Log format**             | `HoodieDeleteBlock`                            | hudi-common          | Tombstone block; v3 = Avro `HoodieDeleteRecordList`                    |
|                            | `HoodieLogBlock.HoodieLogBlockType`            | hudi-common          | `DELETE_BLOCK(":delete")` among data/command block types               |
| **Read path**              | `HoodieFileGroupReader`                        | hudi-common          | Unified slice reader: base + log merge with deletes applied            |
|                            | `KeyBasedFileGroupRecordBuffer`                | hudi-common          | `processDeleteBlock()` / `processNextDeletedRecord()`                  |
|                            | `PositionBasedFileGroupRecordBuffer`           | hudi-common          | Position-keyed variant with key-based fallback                         |
|                            | `BufferedRecord` / `BufferedRecordMergerFactory`| hudi-common         | `isDelete()`, commit-time vs event-time delete resolution              |
|                            | `RecordMergeMode`                              | hudi-common          | `COMMIT_TIME_ORDERING` / `EVENT_TIME_ORDERING` / `CUSTOM`              |
| **Index**                  | `HoodieIndex`                                  | hudi-client-common   | `tagLocation()`, `isGlobal()`                                          |
|                            | `GlobalDeleteKeyGenerator` (+Avro variant)     | spark-client/common  | Empty partition path for global-index delete streams                   |
|                            | `HoodieMetadataPayload`                        | hudi-common          | `createRecordIndexDelete()` — RLI entry removal                        |
|                            | `HoodieBackedTableMetadataWriter`              | hudi-client-common   | Metadata table sync incl. RLI cleanup on partition deletes             |
| **Partition delete**       | `SparkDeletePartitionCommitActionExecutor`     | hudi-spark-client    | replacecommit with `partitionToReplaceFileIds`                         |
|                            | `DeletePartitionUtils`                         | hudi-client-common   | Pending compaction/clustering guard                                    |
|                            | `HoodieReplaceCommitMetadata`                  | hudi-common          | Replaced file IDs per partition                                        |
|                            | `SparkPartitionTTLActionExecutor`              | hudi-spark-client    | TTL-driven automatic partition deletes                                 |
| **Commit metadata**        | `WriteOperationType`                           | hudi-common          | `DELETE`, `DELETE_PREPPED`, `DELETE_PARTITION`                         |
|                            | `HoodieCommitMetadata` / `HoodieWriteStat`     | hudi-common          | `getTotalRecordsDeleted()` / `numDeletes` accounting                   |
| **Maintenance**            | `FileGroupReaderBasedMergeHandle`              | hudi-client-common   | Compaction merge — physically removes tombstoned rows                  |
|                            | `HoodieCompactor`                              | hudi-client-common   | Compaction orchestration                                               |
|                            | `CleanPlanner` / `HoodieCleanConfig`           | hudi-client-common   | Retention; cleans old slices and replaced file groups                  |
| **SQL (Spark)**            | `DeleteHoodieTableCommand`                     | hudi-spark3-common   | `DELETE FROM` command                                                  |
|                            | `ExpressionPayload`                            | hudi-spark-common    | `MERGE ... THEN DELETE` condition evaluation                           |
|                            | `DropPartitionProcedure`                       | hudi-spark           | SQL partition drop                                                     |
| **Flink**                  | `ChangelogModes` / `FlinkOptions`              | hudi-flink           | `RowKind.DELETE` support, `changelog.enabled`                          |
