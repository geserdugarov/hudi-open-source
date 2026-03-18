# Plan: Gate DSv2 Read Feature by Spark Version

## Context

The DSv2 read feature (RFC-98) was implemented for Spark 3.5 in `hudi-spark-common`. However, Hudi supports Spark 3.3, 3.4, 3.5, and 4.0. The DSv2 code uses stable connector interfaces available in all versions, so it compiles everywhere. But it has only been tested/validated for Spark 3.5. We need to:
1. **Prevent** DSv2 read from being activated on Spark 3.3 and 3.4
2. **Ensure** DSv2 read works on Spark 4.0
3. **Update** adapter `isHoodieTable` to recognize `HoodieSparkV2Table` for proper DML resolution on Spark 3.5/4.0

## Changes

### 1. Gate `HoodieCatalog.loadTable()` — catalog/SQL path

**File:** `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/catalog/HoodieCatalog.scala`

At line 153, wrap the `HoodieSparkV2Table` creation with a `gteqSpark3_5` check. On Spark < 3.5, log a warning and fall back to V1:

```scala
import org.apache.hudi.HoodieSparkUtils

// Inside loadTable(), replace the current block:
if (v2ReadEnabled) {
  if (HoodieSparkUtils.gteqSpark3_5) {
    HoodieSparkV2Table(...)
  } else {
    logWarning("DSv2 read (hoodie.datasource.read.use.v2=true) requires Spark 3.5+. " +
      "Falling back to V1 read path.")
    v2Table.v1TableWrapper
  }
} else if (schemaEvolutionEnabled) { ...
```

**Rationale:** Silent fallback with warning. The `use.v2` config might be in shared Spark defaults; an error would break existing queries after a Spark version change.

### 2. Gate `HoodieDataSourceV2.getTable()` — DataFrame API path

**File:** `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieDataSourceV2.scala`

Add a version check at the top of `getTable()`:

```scala
import org.apache.hudi.HoodieSparkUtils

override def getTable(...): Table = {
  if (!HoodieSparkUtils.gteqSpark3_5) {
    throw new HoodieException(
      "The 'hudi_v2' data source requires Spark 3.5 or later. " +
      "Use 'hudi' format instead, or upgrade Spark.")
  }
  // existing code...
}
```

**Rationale:** `format("hudi_v2")` is an explicit per-query choice, so an error is appropriate.

### 3. Update `isHoodieTable` in Spark 3.5 and 4.0 adapters

**Files:**
- `hudi-spark-datasource/hudi-spark3.5.x/src/main/scala/org/apache/spark/sql/adapter/Spark3_5Adapter.scala` (line 73)
- `hudi-spark-datasource/hudi-spark4.0.x/src/main/scala/org/apache/spark/sql/adapter/Spark4_0Adapter.scala` (line 71)

Change `isHoodieTable(V2TableWithV1Fallback)` to also match `HoodieSparkV2Table`:

```scala
def isHoodieTable(v2Table: V2TableWithV1Fallback): Boolean = {
  v2Table.getClass.getName.contains("HoodieInternalV2Table") ||
    v2Table.getClass.getName.contains("HoodieSparkV2Table")
}
```

**Rationale:** Enables `resolveHoodieTable` to recognize DSv2 tables, so DML commands (UPDATE, DELETE, MERGE) resolve correctly for `HoodieSparkV2Table`.

**Not changed:** `Spark3_3Adapter` and `Spark3_4Adapter` — DSv2 is gated off there anyway.

### 4. Add version assumption to DSv2 test classes

**Files** (all in `hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/spark/sql/hudi/v2/`):
- `TestDSv2CoexistenceWithDSv1.scala`
- `TestDSv2CowSnapshotRead.scala`
- `TestDSv2FilterPushdown.scala`
- `TestDSv2MorSnapshotRead.scala`
- `TestDSv2IncrementalRead.scala`
- `TestDSv2CdcRead.scala`
- `TestDSv2Pushdowns.scala`

Add to each test class:

```scala
import org.apache.hudi.HoodieSparkUtils
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach

@BeforeEach
def checkSparkVersion(): Unit = {
  assumeTrue(HoodieSparkUtils.gteqSpark3_5,
    "DSv2 read tests require Spark 3.5 or later")
}
```

These tests are tagged `@Tag("functional")` and extend `SparkClientFunctionalTestHarness`.

## Files NOT changed

- **V2ToV1Fallback rules** (HoodieSpark33/34/35/40DataSourceV2ToV1Fallback): These only handle `HoodieInternalV2Table` (schema evolution). `HoodieSparkV2Table` is gated at creation time, so it never reaches these rules on Spark 3.3/3.4.
- **`HoodieV1OrV2Table` extractor** (HoodieSparkBaseAnalysis.scala:352): Already matches `HoodieSparkV2Table` for DDL commands.
- **META-INF/services**: Kept as-is. The SPI registration for `HoodieDataSourceV2` is in `hudi-spark-common` and shared across versions; the runtime check in `getTable()` handles version gating.
- **No module restructuring**: All DSv2 code stays in `hudi-spark-common`.

## Verification

### Build
```bash
# Verify compilation with each Spark version
mvn clean compile -DskipTests -Dspark3.3
mvn clean compile -DskipTests -Dspark3.4
mvn clean compile -DskipTests -Dspark3.5
mvn clean compile -DskipTests -Dspark4.0
```

### Test
```bash
# DSv2 functional tests should pass on Spark 3.5 (default)
mvn test -Pfunctional-tests -pl hudi-spark-datasource/hudi-spark \
  -Dtest=skipJavaTests \
  -DwildcardSuites=org.apache.spark.sql.hudi.v2

# DSv2 functional tests should be SKIPPED on Spark 3.3/3.4
mvn test -Pfunctional-tests -pl hudi-spark-datasource/hudi-spark -Dspark3.3 \
  -Dtest=skipJavaTests \
  -DwildcardSuites=org.apache.spark.sql.hudi.v2
```
