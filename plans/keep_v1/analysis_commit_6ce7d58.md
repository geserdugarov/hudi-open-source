# Analysis of Commit 6ce7d5839a04 — Behavioral Changes Affecting Existing Code Paths

## Context

Commit `6ce7d5839a04` introduces DSv2 read POC. The user asked to identify all behavioral changes
**excluding** the new `hudi_v2` format and `use.v2` parameter — i.e., side effects on existing V1 behavior.

## Changed Behavior List

### 1. `HoodieV1WriteBuilder.insert()` — Write Logic Rewritten (BREAKING for schema-evolution path)

**File:** `HoodieInternalV2Table.scala:115-150`
**Affects:** Existing schema-evolution write path (when `hoodie.internal.schema.evolution.enable=true`)

**Old code:**
```scala
data.write.format("org.apache.hudi")
  .mode(mode)
  .options(buildHoodieConfig(hoodieCatalogTable) ++
    buildHoodieInsertConfig(hoodieCatalogTable, spark, overwritePartition, overwriteTable, Map.empty, Map.empty))
  .save()
```

**New code:**
```scala
val config = buildHoodieInsertConfig(...)
// Column alignment...
HoodieSparkSqlWriter.write(spark.sqlContext, mode, config, alignedData,
  schemaFromCatalog = Option(catalogSchema))
HoodieSparkSqlWriter.cleanup()
```

**Behavioral differences:**

| Aspect | Old | New | Impact |
|--------|-----|-----|--------|
| `buildHoodieConfig()` called? | YES — includes Hive sync, key generator, ordering fields | **NO — dropped entirely** | **REGRESSION**: Hive sync config, key generator config, ordering fields config are missing from write options. Schema-evolution writes via SQL INSERT will no longer sync to Hive, may use wrong key generator, etc. |
| Write mechanism | `data.write.format("org.apache.hudi").save()` — goes through Spark's full DataSource pipeline | `HoodieSparkSqlWriter.write()` — direct call bypassing Spark DataSource resolution | Different code path; some options that were auto-resolved by Spark DataSource may not be set |
| Column alignment | None | Adds `.select(columns.cast().as(): _*)` when column count matches | Adds a `Project` node to the logical plan; minor overhead |
| `schemaFromCatalog` | Not passed (uses `None` default) | Passes Avro schema from catalog table | Changes schema resolution behavior in `HoodieSparkSqlWriter.getLatestTableSchema()` |
| Cleanup | Handled by Spark DataSource pipeline | Explicit `HoodieSparkSqlWriter.cleanup()` | Should be equivalent |

**Performance impact:** The dropped `buildHoodieConfig()` is a **correctness regression**, not a performance drop. The column alignment adds a negligible `Project` node. The direct `HoodieSparkSqlWriter.write()` call actually removes Spark DataSource resolution overhead (slight improvement).

### 2. `HoodieV1WriteBuilder` Visibility Change

**Old:** `private class HoodieV1WriteBuilder`
**New:** `private[hudi] class HoodieV1WriteBuilder`

**Impact:** No behavioral change for existing paths. Only makes the class accessible to `HoodieSparkV2Table` in the `org.apache.spark.sql.hudi.v2` package (which is within `hudi` scope).

### 3. `HoodieCatalog.loadTable()` — Extra Config Read

**File:** `HoodieCatalog.scala:140-142`

On every `loadTable()` call (for every SQL query on a Hudi table), the new code reads:
```scala
val v2ReadEnabled = spark.sessionState.conf.getConfString(
  DataSourceReadOptions.USE_V2_READ.key,
  DataSourceReadOptions.USE_V2_READ.defaultValue).toBoolean
```

**Impact:** Default is `"false"`, so execution falls through to the existing `schemaEvolutionEnabled` check. This adds one `getConfString()` call per `loadTable()`. **Negligible performance overhead** — `SQLConf` is an in-memory map lookup.

### 4. `DataSourceRegister` Service File — Extra Entry

**File:** `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`
Added: `org.apache.spark.sql.hudi.v2.HoodieDataSourceV2`

Spark scans all `DataSourceRegister` entries during data source resolution. The extra entry means:
- Class loading: `HoodieDataSourceV2` class is loaded during SPI scan (even for `format("hudi")`)
- Short name matching: Spark iterates through all entries to find matching short name

**Performance impact:** **Negligible** — one extra class load at first use, one extra short name comparison per resolution. The short name `"hudi_v2"` won't match `"hudi"`.

### 5. New `TableProvider` Service File

**File:** `META-INF/services/org.apache.spark.sql.connector.catalog.TableProvider` (new file)

Spark may scan `TableProvider` entries during catalog resolution. This file didn't exist before.

**Performance impact:** **Negligible** — similar to above, one extra SPI scan entry.

### 6. `HoodieSparkBaseAnalysis.HoodieV1OrV2Table` — Extra Pattern Match Case

**File:** `HoodieSparkBaseAnalysis.scala:356`
Added: `case v2: HoodieSparkV2Table => v2.catalogTable`

This only adds a new `case` at the end of the `match` block. When no `HoodieSparkV2Table` exists (no V2 usage), this case never matches. Pattern matching evaluates cases in order, so the existing cases (`V1Table`, `HoodieInternalV2Table`) are checked first.

**Performance impact:** **Zero** — the new case is only checked if previous cases don't match, which was already the `case _ => None` fallback.

### 7. `Spark3_5Adapter` / `Spark4_0Adapter` — `isHoodieTable(V2TableWithV1Fallback)` NOT Updated

**File:** `Spark3_5Adapter.scala:73-75`, `Spark4_0Adapter.scala:71-73`

```scala
def isHoodieTable(v2Table: V2TableWithV1Fallback): Boolean = {
  v2Table.getClass.getName.contains("HoodieInternalV2Table")
}
```

This was **NOT changed** in the commit. It only recognizes `HoodieInternalV2Table` by class name string. `HoodieSparkV2Table` will NOT be recognized.

**Impact on existing paths:** None — `HoodieSparkV2Table` only appears when V2 is explicitly enabled. This is a correctness gap for the new V2 feature (time travel won't work), but doesn't affect existing V1 paths.

---

## Summary: Changes Affecting Existing Code Paths (No "hudi_v2" / "use.v2")

| # | Change | Affects existing paths? | Performance impact | Correctness impact |
|---|--------|------------------------|-------------------|-------------------|
| 1 | `HoodieV1WriteBuilder.insert()` rewrite | **YES** (schema-evolution writes) | Negligible (saves DataSource resolution overhead, adds `Project` node) | **REGRESSION: `buildHoodieConfig()` dropped — loses Hive sync, key generator, ordering field configs** |
| 2 | `HoodieV1WriteBuilder` visibility | No | None | None |
| 3 | Extra config read in `loadTable()` | Yes (every SQL query) | Negligible | None |
| 4 | Extra `DataSourceRegister` entry | Yes (data source resolution) | Negligible | None |
| 5 | New `TableProvider` service file | Yes (SPI scan) | Negligible | None |
| 6 | Extra pattern match case | No | Zero | None |
| 7 | Adapter `isHoodieTable` not updated | No (only affects V2) | N/A | Gap for V2 only |

## V1/V2 Correctness Analysis

**Is there any wrong telling to Spark that a table is V1 or V2?**

- **When `use.v2=false` (default):** `HoodieCatalog.loadTable()` returns `v2Table.v1TableWrapper` (a `V1Table`). This is correct — Spark treats it as pure V1.

- **When schema evolution enabled:** `HoodieCatalog.loadTable()` returns `HoodieInternalV2Table` (which has `V2TableWithV1Fallback`). This is unchanged from before. Spark recognizes it as V2 for reads but falls back to V1 for writes. Correct.

- **The only issue:** `HoodieSparkV2Table` implements both `SupportsRead` and `V2TableWithV1Fallback`. When it appears in a `DataSourceV2Relation`, the Spark 3.5/4.0 adapters' `isHoodieTable` won't recognize it (only checks for `HoodieInternalV2Table` by name). This means `resolveHoodieTable` returns `None` for these tables. **This only affects the new V2 path**, not existing V1 behavior.

**No existing table is incorrectly told to Spark as V1 or V2 by this commit.**

## Recommendation

**Critical fix needed:** The `HoodieV1WriteBuilder.insert()` rewrite drops `buildHoodieConfig()`. This must be restored for the schema-evolution write path. The fix should either:
1. Merge `buildHoodieConfig()` output into the `config` map before calling `HoodieSparkSqlWriter.write()`, OR
2. Keep the old `data.write.format("org.apache.hudi").save()` approach for the existing schema-evolution path and only use the new approach for the V2 path
