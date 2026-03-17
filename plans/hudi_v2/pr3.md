# Plan: Update PR2 Status + Create PR3 Plan

## Context

PR2 (CoW Snapshot Read) is fully implemented on branch `hudi-worktree-v2`. All 6 DSv2 component files are complete (not stubs), and test coverage exists via `TestDSv2CowSnapshotRead.scala` (11 tests) and updated `TestDSv2CoexistenceWithDSv1.scala` (13 tests). The DSv2 path now reads actual data from CoW tables via `HoodieFileIndex` + `SparkColumnarFileReader`.

PR3 adds filter pushdown so that partition pruning and data skipping work through the DSv2 path, matching the DSv1 behavior.

## Task 1: Update `incremental_pr_plan.md` — Mark PR2 as DONE

**File:** `plans/hudi_v2/incremental_pr_plan.md`

Changes:
- Line 142: `**Status: NOT STARTED**` → `**Status: DONE**`
- Line 148: `**Depends on:** PR 1 (DONE)` → `**Depends on:** PR 1 (DONE)` (no change needed)
- Update verification checklist items (lines 188-193): mark all `[ ]` as `[x]`
- Update summary dependency graph (lines 371-372): `PR 2: CoW Snapshot Read  ⬜ NOT STARTED` → `PR 2: CoW Snapshot Read  ✅ DONE`
- Update PR3 depends-on (line 204): `**Depends on:** PR 2 (NOT STARTED)` → `**Depends on:** PR 2 (DONE)`

## Task 2: Create `plans/hudi_v2/pr3.md` — CoW Filter Pushdown

### Context

Currently `HoodieScanBuilder.pushFilters()` returns all filters as post-scan and passes `Seq.empty` to `fileIndex.filterFileSlices()`. This means no partition pruning or data skipping (bloom filters, column stats) occurs. PR3 wires filter pushdown so that:
1. Filters convertible to Catalyst expressions are accepted
2. Partition filters prune partitions via `HoodieFileIndex`
3. Data filters enable data skipping via metadata table indices

### Approach: Reuse DSv1 Pattern

The DSv1 path (`HoodieBaseRelation`) already does exactly this:
1. `convertToExpressions(filters)` — uses `HoodieCatalystExpressionUtils.convertToCatalystExpression()` to convert each `Filter` → `Expression`
2. `isPartitionPredicate(expr)` — checks if all column references are partition columns
3. Passes `(partitionFilters, dataFilters)` to `fileIndex.filterFileSlices()`

PR3 copies this pattern into `HoodieScanBuilder`.

### File Changes

#### 1. `HoodieScanBuilder.scala` (primary change)

**Path:** `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieScanBuilder.scala`

Changes:
- Add import for `HoodieCatalystExpressionUtils.convertToCatalystExpression`
- Add import for `Expression`, `SubqueryExpression`
- Add fields: `private var _pushedFilters: Array[Filter] = Array.empty`
- Add fields: `private var partitionFilterExprs: Seq[Expression] = Seq.empty`
- Add fields: `private var dataFilterExprs: Seq[Expression] = Seq.empty`

**`pushFilters()` implementation:**
```scala
override def pushFilters(filters: Array[Filter]): Array[Filter] = {
  // fileIndex must be created here to know partition columns
  // Actually, we can defer fileIndex creation. We just need partitionSchema.
  // Create a lightweight fileIndex to get partition schema, or store it from build().
  // Better: create fileIndex lazily, use it in both pushFilters and build.

  val (pushed, postScan) = filters.partition { f =>
    convertToCatalystExpression(f, tableSchema).isDefined
  }

  // Convert pushed filters to Catalyst expressions
  val expressions = pushed.flatMap(f => convertToCatalystExpression(f, tableSchema))

  // Split into partition vs data filters (need fileIndex for partitionSchema)
  val partFieldNames = lazyFileIndex.partitionSchema.fieldNames.toSet
  val resolver = spark.sessionState.analyzer.resolver

  partitionFilterExprs = expressions.filter { expr =>
    expr.references.forall(r => partFieldNames.exists(resolver(r.name, _))) &&
      !SubqueryExpression.hasSubquery(expr)
  }
  dataFilterExprs = expressions.filterNot(partitionFilterExprs.contains)

  _pushedFilters = pushed
  postScan  // return only non-pushable filters
}
```

**Key design decision:** Create `HoodieFileIndex` lazily (via `lazy val`) so it can be shared between `pushFilters()` (needs partition schema) and `build()` (needs file listing). Currently fileIndex is created inside `build()` — it needs to move to a lazy val.

**`build()` changes:**
```scala
// Replace: val fileIndex = HoodieFileIndex(...)
// With: use lazyFileIndex (already created)

// Replace: fileIndex.filterFileSlices(Seq.empty, Seq.empty)
// With:    lazyFileIndex.filterFileSlices(dataFilterExprs, partitionFilterExprs)
```

#### 2. `HoodieBatchScan.scala` (minor)

**Path:** `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieBatchScan.scala`

No changes needed. `HoodieBatchScan` already receives pre-filtered partitions from `HoodieScanBuilder.build()`. The filtering happens upstream.

#### 3. No other production files need changes

### New Test File

**Path:** `hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/spark/sql/hudi/v2/TestDSv2FilterPushdown.scala`

Extends `SparkClientFunctionalTestHarness`, `@Tag("functional")`.

**Test cases:**

1. **testPartitionPruning** — Create partitioned CoW table with 3 partitions (US, UK, FR), write data. Read via DSv2 with `WHERE country = 'US'`. Verify only US data returned. Verify via EXPLAIN that pushed filters appear.

2. **testPartitionPruningViaSql** — Same as above but via SQL `SELECT * FROM table WHERE country = 'US'` with `use.v2=true`.

3. **testDataFilterPushdown** — Create table with multiple files. Read via DSv2 with `WHERE id > 5`. Verify correct results. (Data skipping effectiveness depends on metadata table config, so mainly verify correctness.)

4. **testMixedPartitionAndDataFilters** — `WHERE country = 'US' AND name = 'Alice'`. Verify partition filter prunes partitions, data filter is also pushed.

5. **testUnconvertibleFilterReturnedAsPostScan** — Use a filter that can't be converted (if any exist). Verify results still correct (Spark applies it post-scan).

6. **testExplainShowsPushedFilters** — Verify EXPLAIN output includes pushed filters in `BatchScanExec`.

7. **testDsv1VsDsv2FilterResults** — Same table, same WHERE clause, compare DSv1 vs DSv2 results for correctness.

8. **testNoFilterScansAllPartitions** — Read without WHERE clause, verify all data returned (baseline).

9. **testInFilterOnPartitionColumn** — `WHERE country IN ('US', 'UK')`. Verify only 2 partitions returned.

10. **testIsNullFilter** — `WHERE nullable_col IS NULL`. Verify pushed and correct.

### Reused Utilities

| Utility | Location | Usage |
|---------|----------|-------|
| `HoodieCatalystExpressionUtils.convertToCatalystExpression()` | `hudi-client/hudi-spark-client` | Convert `Filter` → `Expression` |
| `HoodieFileIndex.filterFileSlices(dataFilters, partitionFilters)` | `hudi-spark-common` | Apply partition pruning + data skipping |
| `HoodieFileIndex.partitionSchema` | `hudi-spark-common` | Determine partition column names |
| `SubqueryExpression.hasSubquery()` | Spark Catalyst | Exclude subquery predicates from partition filters |

### Implementation Order

1. Modify `HoodieScanBuilder.scala` — lazy fileIndex, implement `pushFilters()` / `pushedFilters()`, update `build()` to pass filters
2. Create `TestDSv2FilterPushdown.scala` — new test file
3. Update `plans/hudi_v2/incremental_pr_plan.md` — mark PR3 status

### Verification

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

# Run filter pushdown tests
JAVA_HOME=${JAVA_HOME_PATH} mvn test -Pfunctional-tests \
  -Dmaven.repo.local=${MAVEN_REPO_PATH} \
  -Djava11 -Djava.version=11 -Dspark3.5 -Dflink1.20 \
  -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true \
  -Dmaven.wagon.httpconnectionManager.ttlSeconds=25 -Dmaven.wagon.http.retryHandler.count=5 \
  -pl hudi-spark-datasource/hudi-spark \
  -Dtest=TestDSv2FilterPushdown

# Run existing tests to verify no regressions
JAVA_HOME=${JAVA_HOME_PATH} mvn test -Pfunctional-tests \
  -Dmaven.repo.local=${MAVEN_REPO_PATH} \
  -Djava11 -Djava.version=11 -Dspark3.5 -Dflink1.20 \
  -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true \
  -Dmaven.wagon.httpconnectionManager.ttlSeconds=25 -Dmaven.wagon.http.retryHandler.count=5 \
  -pl hudi-spark-datasource/hudi-spark \
  -Dtest=TestDSv2CowSnapshotRead,TestDSv2CoexistenceWithDSv1
```

### Key Risks

1. **Lazy fileIndex lifecycle** — `pushFilters()` is called before `build()` by Spark optimizer. The `lazy val` pattern ensures fileIndex is created once and shared. Risk: if Spark calls `pushFilters()` multiple times, the partition/data filter split is idempotent so this is safe.

2. **Filter conversion completeness** — `convertToCatalystExpression()` doesn't support all filter types (e.g., complex nested predicates). Unconverted filters are returned as post-scan, so correctness is preserved. This matches DSv1 behavior.

3. **Partition column case sensitivity** — Using `spark.sessionState.analyzer.resolver` (same as DSv1) handles case-insensitive column matching correctly.
