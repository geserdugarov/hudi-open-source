# Spark Integration Architecture

## Three-Layer Module Structure

The Paimon Spark integration uses a three-layer architecture to support multiple Spark versions while maximizing code reuse:

1. **Version-independent layer** (`paimon-spark-common`) -- Contains all code that works across every supported Spark version: catalog implementations, scan/read logic, write support, SQL extensions grammar, stored procedures, data type conversions, and Catalyst analysis rules.

2. **Major-version-common layer** (`paimon-spark3-common`, `paimon-spark4-common`) -- Contains code specific to a major Spark version. For Spark 3.x this includes `PaimonSpark3SqlExtensionsParser`, `Spark3ResolutionRules`, `Spark3InternalRow`, `Spark3ArrayData`, and `Spark3Shim`. For Spark 4.x, the analogous `Spark4*` classes plus an additional `spark-sql-api` dependency.

3. **Version-specific shim layer** (`paimon-spark-3.2`, `paimon-spark-3.3`, `paimon-spark-3.4`, `paimon-spark-3.5`, `paimon-spark-4.0`) -- Contains overrides for APIs that differ in a specific minor Spark version. Each shim module produces the final deploy-ready JAR for that Spark version.

## Module Dependency Chain

```
paimon-bundle
    |
    v
paimon-spark-common  (version-independent: catalogs, scans, writes, SQL extensions)
    |
    +---------------------------+
    |                           |
    v                           v
paimon-spark3-common         paimon-spark4-common
(Scala 2.12, Spark 3.x)     (Scala 2.13, Spark 4.x, JDK 17)
    |                           |
    +-------+-------+-------+  |
    |       |       |       |  |
    v       v       v       v  v
spark-3.2 spark-3.3 spark-3.4 spark-3.5 spark-4.0
```

Each shim module's shaded JAR bundles its parent (e.g. `paimon-spark-3.5` bundles `paimon-spark3-common`, which in turn bundles `paimon-spark-common` and `paimon-bundle`), producing a single self-contained artifact.

## Shim Pattern Details

### SparkShim Trait

The `SparkShim` trait is defined in `paimon-spark-common` at `org.apache.spark.sql.paimon.shims.SparkShim`. It declares methods with incompatible implementations between Spark 3 and Spark 4:

- `createSparkParser` -- Creates the version-appropriate SQL extensions parser
- `createCustomResolution` -- Creates version-specific Catalyst resolution rules
- `createSparkInternalRow` / `createSparkArrayData` -- Creates version-specific internal row and array adapters
- `createTable` -- Delegates to `TableCatalog.createTable` with version-appropriate signature
- `createCTERelationRef` -- Handles CTE constructor differences between versions
- `supportsHashAggregate` / `supportsObjectHashAggregate` -- Aggregation support checks
- `createMergeIntoTable` -- MERGE INTO with version-specific constructor
- `toPaimonVariant` / `isSparkVariantType` / `SparkVariantType` -- Variant type support (Spark 4 only)

`SparkShimLoader` (`org.apache.spark.sql.paimon.shims.SparkShimLoader`) loads the implementation at runtime via `java.util.ServiceLoader`. Exactly one `SparkShim` implementation must be on the classpath:

- `Spark3Shim` -- registered in `paimon-spark3-common/src/main/resources/META-INF/services/org.apache.spark.sql.paimon.shims.SparkShim`
- `Spark4Shim` -- registered in `paimon-spark4-common/src/main/resources/META-INF/services/org.apache.spark.sql.paimon.shims.SparkShim`

### MinorVersionShim Object

`MinorVersionShim` is a Scala `object` defined in `paimon-spark3-common` at `org.apache.spark.sql.paimon.shims.MinorVersionShim`. It provides factory methods for Catalyst plan nodes whose constructors changed between Spark 3.x minor versions (e.g. `createCTERelationRef`, `createMergeIntoTable`).

The default implementation in `paimon-spark3-common` targets the latest Spark 3.x APIs. The `paimon-spark-3.2` and `paimon-spark-3.3` modules shadow this object on the classpath with their own versions that match the older Spark 3.2/3.3 constructors. Because Scala objects are loaded by classname, the shim module's version takes precedence when it appears first on the classpath.

## Key Classes

### Catalog Layer

- **`SparkCatalog`** (`org.apache.paimon.spark.SparkCatalog`) -- A dedicated Paimon catalog for Spark. Implements `TableCatalog`, `SupportsNamespaces`, `FunctionCatalog`, and view support. Use this when Paimon is configured as a named catalog.

- **`SparkGenericCatalog`** (`org.apache.paimon.spark.SparkGenericCatalog`) -- Implements `CatalogExtension` to serve as Spark's default session catalog (`spark_catalog`). Delegates non-Paimon tables to the underlying Hive/in-memory session catalog while routing Paimon tables through the Paimon catalog.

### Table Layer

- **`PaimonSparkTableBase`** (`org.apache.paimon.spark.PaimonSparkTableBase`) -- Base trait defining common Spark `Table` capabilities for Paimon tables.

- **`SparkTable`** (`org.apache.paimon.spark.SparkTable`) -- Concrete Spark `Table` implementation wrapping a Paimon `Table`. Defined in `paimon-spark-common` and overridden in `paimon-spark-3.2`, `paimon-spark-3.3`, and `paimon-spark-3.4` to handle API differences (e.g. `TableCatalogCapability`, `SupportsReportOrdering`).

### Data Source Layer

- **`SparkSource`** (`org.apache.paimon.spark.SparkSource`) -- Registered as `"paimon"` via SPI (`META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`). Implements `DataSourceRegister`, `SessionConfigSupport`, `CreatableRelationProvider`, and `StreamSinkProvider`.

### SQL Extensions

- **`PaimonSparkSessionExtensions`** (`org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions`) -- Injects custom parser extensions, Catalyst analysis rules (e.g. `PaimonAnalysis`, `PaimonMergeInto`, `PaimonProcedureResolver`), optimizer rules (`MergePaimonScalarSubqueries`, `OptimizeMetadataOnlyDeleteFromPaimonTable`), and planner strategies (`PaimonStrategy`, `OldCompatibleStrategy`).

## Version-Specific Override Summary

The number of class overrides decreases with newer Spark versions, reflecting increasing API alignment:

| Module | Overrides |
|--------|-----------|
| `paimon-spark-3.2` | `PaimonBaseScanBuilder`, `PaimonScan`, `SparkTable`, `Compatibility`, `OldCompatibleStrategy`, runtime filtering, `TableCatalogCapability` stub, `MinorVersionShim` |
| `paimon-spark-3.3` | `PaimonScan`, `SparkTable`, `Compatibility`, merge-into analysis, `MergePaimonScalarSubqueries`, `OldCompatibleStrategy`, runtime filtering, `SQLConfUtils`, `TypeUtils`, `TableCatalogCapability` stub, `SupportsReportOrdering` stub, `MinorVersionShim` |
| `paimon-spark-3.4` | `SparkTable`, `MergePaimonScalarSubqueries`, `OldCompatibleStrategy` |
| `paimon-spark-3.5` | `MergePaimonScalarSubqueries` |
| `paimon-spark-4.0` | `MergePaimonScalarSubqueries` |

## Testing Strategy

### Test-JAR Pattern

The `paimon-spark-ut` module contains only test code (no main sources). It publishes a `test-jar` artifact containing shared test base classes and test suites:

- `PaimonSparkTestBase` -- Base class for Spark integration tests
- `PaimonHiveTestBase` -- Base class for Hive-related Spark tests
- Test suites covering DDL, reads, writes, merge-into, procedures, schema evolution, and more

### Test Inheritance

Each version-specific shim module (`paimon-spark-3.2` through `paimon-spark-4.0`) depends on `paimon-spark-ut` as a `test-jar` dependency. Concrete test classes in each shim extend the `*TestBase` classes from `paimon-spark-ut`, causing the full test suite to run against that specific Spark version without code duplication.

### Running Tests

```bash
# Run all tests for a specific Spark version
mvn test verify -pl paimon-spark/paimon-spark-3.5 -Pspark3

# Run a single test class
mvn test -pl paimon-spark/paimon-spark-3.5 -Dtest=MyTestClass -Pspark3

# Run Spark 4.0 tests (requires JDK 17)
mvn test verify -pl paimon-spark/paimon-spark-4.0 -Pspark4
```

## Related Documentation

- [Spark Integration Overview](spark-integration-overview.md) -- supported versions, module summary, key capabilities
- [Spark Integration Configuration](spark-integration-configuration.md) -- build profiles, connector/catalog options, and packaging details
