# Spark Integration Overview

## Introduction

The `paimon-spark` module provides Apache Paimon's integration with Apache Spark. It enables Spark SQL, DataFrame, and Structured Streaming access to Paimon tables using the data source name `"paimon"`. The entry point is `SparkSource` (`org.apache.paimon.spark.SparkSource`), which registers as a Spark `DataSourceRegister` via SPI.

## Supported Spark Versions

| Spark Version | Scala Version | JDK Requirement | Maven Profile | Module |
|---------------|---------------|-----------------|---------------|--------|
| 3.2.4 | 2.12 | JDK 8+ | `spark3` | `paimon-spark-3.2` |
| 3.3.4 | 2.12 | JDK 8+ | `spark3` | `paimon-spark-3.3` |
| 3.4.3 | 2.12 | JDK 8+ | `spark3` | `paimon-spark-3.4` |
| 3.5.8 | 2.12 | JDK 8+ | `spark3` | `paimon-spark-3.5` |
| 4.0.2 | 2.13 | JDK 17 | `spark4` | `paimon-spark-4.0` |

## Module Summary

| Module | Description |
|--------|-------------|
| `paimon-spark` | Parent aggregator module that configures the Scala compiler, Scalatest runner, and shared dependencies. |
| `paimon-spark-common` | Version-independent shared code: catalog implementations, scan/read logic, write support, ANTLR4-based SQL extensions grammar, stored procedures, data type conversions, and Catalyst analysis rules. |
| `paimon-spark3-common` | Spark 3.x-specific shared code (Scala 2.12): SQL extensions parser, resolution rules, Spark 3 internal row/array adapters, and `Spark3Shim` implementation. |
| `paimon-spark4-common` | Spark 4.x-specific shared code (Scala 2.13, JDK 17): SQL extensions parser, resolution rules, Spark 4 internal row/array adapters, and `Spark4Shim` implementation. |
| `paimon-spark-3.2` | Version-specific shim for Spark 3.2.4: overrides for `PaimonBaseScanBuilder`, `PaimonScan`, `SparkTable`, `Compatibility`, `OldCompatibleStrategy`, runtime filtering, and a `TableCatalogCapability` stub. |
| `paimon-spark-3.3` | Version-specific shim for Spark 3.3.4: the most overrides among 3.x shims, including `PaimonScan`, `SparkTable`, `Compatibility`, merge-into analysis, `MergePaimonScalarSubqueries`, `OldCompatibleStrategy`, runtime filtering, `SQLConfUtils`, `TypeUtils`, and stubs for `TableCatalogCapability` and `SupportsReportOrdering`. |
| `paimon-spark-3.4` | Version-specific shim for Spark 3.4.3: overrides for `SparkTable`, `MergePaimonScalarSubqueries`, and `OldCompatibleStrategy`. |
| `paimon-spark-3.5` | Version-specific shim for Spark 3.5.8: a single override for `MergePaimonScalarSubqueries`. |
| `paimon-spark-4.0` | Version-specific shim for Spark 4.0.2: a single override for `MergePaimonScalarSubqueries`; also includes `paimon-lucene` as a test dependency. |
| `paimon-spark-ut` | Shared test base classes (test-jar only): `PaimonSparkTestBase`, `PaimonHiveTestBase`, and test suites covering DDL, reads, writes, merge-into, procedures, schema evolution, and more. |

## Key Capabilities

- **Catalog integration** via `SparkCatalog` and `SparkGenericCatalog`
- **Batch reads** with partition and bucket pruning
- **Streaming reads** with configurable rate limiting (`maxFilesPerTrigger`, `maxBytesPerTrigger`, `maxRowsPerTrigger`)
- **Batch writes** using both V1 (`CreatableRelationProvider`) and V2 (`BatchWrite`) APIs
- **Overwrite and truncation** support
- **Metadata columns** exposure
- **SQL extensions** via `PaimonSparkSessionExtensions` (custom parser, analysis rules, optimizer rules, planner strategies)
- **Stored procedures** for administrative operations
- **Streaming writes** via `PaimonSink` (Append and Complete output modes)
- **Schema merge** on write (`write.merge-schema` option)
- **Changelog reads** (`read.changelog` option adds a rowkind column)
- **Split size adjustment** with column pruning (`source.split.target-size-with-column-pruning`)

## Related Documentation

- [Spark Integration Architecture](spark-integration-architecture.md) -- module structure, shim pattern, key classes, and testing strategy
- [Spark Integration Configuration](spark-integration-configuration.md) -- build profiles, connector/catalog options, and packaging details
