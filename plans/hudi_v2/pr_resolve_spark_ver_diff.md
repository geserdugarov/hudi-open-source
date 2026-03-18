# Plan: Make HoodieScanBuilder compatible with Spark 3.3, 3.4, 3.5, and 4.0

## Context

Commit `436759d` added a Spark 3.5+ requirement for the `hudi_v2` data source. The root cause is that `SupportsPushDownLimit.isPartiallyPushed()` is a **default method added in Spark 3.4** (returning `false`). It does NOT exist in Spark 3.3. The current code has `override def isPartiallyPushed(): Boolean = true`, which:
- Fails on Spark 3.3: nothing to override (method doesn't exist in the interface)
- Works on Spark 3.4+: overrides the default method

The goal is to support all Spark versions (3.3, 3.4, 3.5, 4.0) without version-specific subclasses or SparkAdapter changes.

## Approach: Java interface with default method

Create a Java interface `PartialLimitPushDown extends SupportsPushDownLimit` that provides `isPartiallyPushed()` as a default method returning `true`. This avoids the Scala `override` keyword issue entirely:

- **Spark 3.3**: `SupportsPushDownLimit` has no `isPartiallyPushed`. Our interface adds it as a new default method. Compiles fine. Spark 3.3 never calls it (doesn't know about it). Harmless.
- **Spark 3.4+**: `SupportsPushDownLimit` has `default boolean isPartiallyPushed() { return false; }`. Our interface overrides it with `return true`. Java doesn't require `@Override` annotation. The more specific interface's default wins at the JVM level.

This requires only 3 changes total, no new modules or version-specific files.

## Changes

### 1. Create new Java interface
**File**: `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/spark/sql/hudi/v2/PartialLimitPushDown.java`

```java
package org.apache.spark.sql.hudi.v2;

import org.apache.spark.sql.connector.read.SupportsPushDownLimit;

/**
 * Extension of {@link SupportsPushDownLimit} that marks limit pushdown as partial.
 *
 * Spark 3.4+ added {@code isPartiallyPushed()} as a default method (returning false).
 * Spark 3.3 does not have this method. By providing the default here in a Java interface,
 * we avoid the Scala {@code override} keyword issue: Java default methods don't require
 * {@code @Override}, so this compiles against both Spark 3.3 (new method) and 3.4+ (override).
 *
 * When {@code isPartiallyPushed()} returns true, Spark adds a final LocalLimit on top of the
 * scan, which is necessary because Hudi's limit pushdown is best-effort (per-partition).
 */
public interface PartialLimitPushDown extends SupportsPushDownLimit {
  default boolean isPartiallyPushed() {
    return true;
  }
}
```

### 2. Update HoodieScanBuilder
**File**: `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieScanBuilder.scala`

- Replace `with SupportsPushDownLimit` → `with PartialLimitPushDown`
- Remove `import ... SupportsPushDownLimit` (no longer directly needed)
- Remove `override def isPartiallyPushed(): Boolean = true` (line 135)
- Keep `override def pushLimit(limit: Int): Boolean` as-is (pushLimit is abstract in all Spark versions)

### 3. Remove Spark 3.5+ requirement
**File**: `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieDataSourceV2.scala`

- Remove the version check block (lines 53-57):
  ```scala
  if (!HoodieSparkUtils.gteqSpark3_5) {
    throw new HoodieException(...)
  }
  ```
- Remove the unused `HoodieSparkUtils` import if it becomes unused

## Files to modify
1. **NEW**: `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/spark/sql/hudi/v2/PartialLimitPushDown.java`
2. **EDIT**: `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieScanBuilder.scala`
3. **EDIT**: `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/HoodieDataSourceV2.scala`

## Why not the abstract base class approach?

The user's initial idea (abstract base class + version-specific subclasses) would work but is more complex:
- Requires creating `BaseHoodieScanBuilder` + 4 version-specific `HoodieScanBuilder` files
- Requires a factory method in `SparkAdapter` (since `hudi-spark-common` can't reference version-specific modules)
- Requires modifying all 4 version-specific adapters

The Java interface approach achieves the same result with 1 new file and 2 edits, no SparkAdapter changes, and no version-specific modules touched.

## Verification

1. Build with Spark 3.5 (default): `mvn clean package -DskipTests -pl hudi-spark-datasource/hudi-spark-common -am`
2. Build with Spark 3.3: `mvn clean package -DskipTests -pl hudi-spark-datasource/hudi-spark-common -am -Dspark3.3`
3. Build with Spark 4.0: `mvn clean package -DskipTests -pl hudi-spark-datasource/hudi-spark-common -am -Dspark4.0`
4. Run DSv2 pushdown tests: `mvn test -Punit-tests -pl hudi-spark-datasource/hudi-spark -Dtest=skipJavaTests -DwildcardSuites=org.apache.spark.sql.hudi.v2.TestDSv2Pushdowns`
