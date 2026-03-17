# PR 5: Incremental & CDC Queries

**Status: IN PROGRESS**

**Goal:** Support incremental queries (`hoodie.datasource.query.type=incremental`) and CDC queries
through the DSv2 path, achieving feature parity with DSv1 for all three query types:
`snapshot` (already done), `read_optimized`, and `incremental` (both `latest_state` and `cdc` formats).

**Branch:** `dsv2-read-05-incremental-cdc`

**Depends on:** PR 4 (DONE)

## Approach

1. **Read-optimized** (trivial): treat MoR tables like CoW by setting `includeLogFiles = false`
2. **Incremental (latest_state)**: use `IncrementalQueryAnalyzer` + `HoodieTableFileSystemView`
   for file listing, add commit time record filtering in partition readers
3. **Incremental (cdc)**: use `HoodieCDCExtractor` for CDC file splits, wrap `CDCFileGroupIterator`
   in new CDC-specific partition reader classes

## File Changes

### Modified Files

1. **`HoodieScanBuilder.scala`** - Query type dispatch + incremental file listing + CDC scan building
2. **`HoodieBatchScan.scala`** - Add `includedCommitTimes` parameter
3. **`HoodiePartitionReaderFactory.scala`** - Pass `includedCommitTimes`
4. **`HoodiePartitionReader.scala`** - Add commit time row filtering
5. **`HoodieMorPartitionReader.scala`** - Add commit time row filtering

### New Files

6. **`HoodieCdcInputPartition.scala`** - CDC partition holding `HoodieCDCFileGroupSplit`
7. **`HoodieCdcBatchScan.scala`** - CDC scan
8. **`HoodieCdcPartitionReaderFactory.scala`** - CDC reader factory
9. **`HoodieCdcPartitionReader.scala`** - CDC reader wrapping `CDCFileGroupIterator`

### New Test Files

10. **`TestDSv2IncrementalRead.scala`** - Incremental + read_optimized tests
11. **`TestDSv2CdcRead.scala`** - CDC tests

## Key Design Decisions

1. **Commit time row filtering**: `includedCommitTimes: Option[Set[String]]` passed through the chain.
   Readers filter rows by `_hoodie_commit_time IN includedCommitTimes` after reading/merging.
2. **Schema augmentation for incremental**: `buildIncrementalScan()` augments `requiredDataSchema`
   with mandatory meta fields. After filtering, `UnsafeProjection` strips them if the user didn't request them.
3. **CDC as separate scan classes**: Fundamentally different schema and file model.
4. **Read-optimized as trivial variant**: Just set `includeLogFiles = false`.
