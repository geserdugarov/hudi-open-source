# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Hudi is an open data lakehouse platform built on a high-performance open table format. It supports ingestion, indexing, storage, serving, and transformation of data across cloud environments. The project is a multi-module Maven build (Java 11+, Scala 2.12/2.13) with engine integrations for Apache Spark, Apache Flink, Presto, and Trino.

## Build Commands

```bash
# Full build (skip tests for speed)
mvn clean package -DskipTests

# Build a specific module (with dependencies)
mvn clean package -DskipTests -pl hudi-common -am

# Build with a specific Spark version (default is Spark 3.5 / Scala 2.12)
mvn clean package -DskipTests -Dspark3.4
mvn clean package -DskipTests -Dspark3.5 -Dscala-2.13
mvn clean package -DskipTests -Dspark4.0          # requires Java 17

# Build with a specific Flink version (default is Flink 1.20)
mvn clean package -DskipTests -Dflink1.18
mvn clean package -DskipTests -Dflink2.1

# Build integration test bundle
mvn clean package -DskipTests -Dintegration-tests
```

## Running Tests

Tests are tagged and split across Maven profiles. Tests without a profile are skipped by default.

```bash
# Unit tests (excludes @Tag("functional") tests)
mvn test -Punit-tests

# Functional tests only (@Tag("functional"))
mvn test -Pfunctional-tests

# Integration tests
mvn verify -Pintegration-tests

# Run tests for a specific module
mvn test -Punit-tests -pl hudi-common

# Run a single Java test class
mvn test -Punit-tests -pl hudi-common -Dtest=TestHoodieCommitMetadata

# Run a single Java test method
mvn test -Punit-tests -pl hudi-common -Dtest=TestHoodieCommitMetadata#testMethodName

# Run Scala tests by suite (for Spark datasource modules)
mvn test -Punit-tests -pl hudi-spark-datasource/hudi-spark -DwildcardSuites=org.apache.spark.sql.hudi.dml

# Skip Java tests when running only Scala tests
mvn test -Punit-tests -pl hudi-spark-datasource/hudi-spark -Dtest=skipJavaTests -DwildcardSuites=org.apache.spark.sql.hudi.dml

# Skip Scala tests when running only Java tests
mvn test -Punit-tests -pl hudi-spark-datasource/hudi-spark -DwildcardSuites=skipScalaTests
```

## Module Architecture

### Core Modules

- **hudi-io** - Storage abstraction layer (`HoodieStorage`, `StoragePath`, `Option<T>`). No Spark/Flink dependencies.
- **hudi-common** - Core data model, timeline, metadata, file system view. Depends on hudi-io.
- **hudi-client/hudi-client-common** - Engine-agnostic write client, table operations, index framework. No Scala imports allowed (enforced by checkstyle import-control).
- **hudi-client/hudi-spark-client** - Spark-specific write client (`SparkRDDWriteClient`).
- **hudi-client/hudi-flink-client** - Flink-specific write client (`HoodieFlinkWriteClient`).
- **hudi-client/hudi-java-client** - Standalone Java write client.

### Engine Integration Modules

- **hudi-spark-datasource/hudi-spark-common** - Shared Spark SQL integration code.
- **hudi-spark-datasource/hudi-spark3-common** - Spark 3.x shared code.
- **hudi-spark-datasource/hudi-spark3.5.x** (and 3.3.x, 3.4.x, 4.0.x) - Version-specific Spark adapters.
- **hudi-spark-datasource/hudi-spark** - Main Spark datasource (SQL writer, reader, file index).
- **hudi-flink-datasource/hudi-flink** - Main Flink datasource (streaming sink/source).
- **hudi-flink-datasource/hudi-flink1.20.x** (and 1.17-2.1) - Version-specific Flink adapters.

### Supporting Modules

- **hudi-utilities** - Ingestion tools (HoodieStreamer/DeltaStreamer), compaction, clustering utilities.
- **hudi-sync** - Metastore sync (Hive, DataHub, ADB). Common interface in `hudi-sync-common`.
- **hudi-hadoop-mr** - MapReduce InputFormat for Hudi tables.
- **hudi-timeline-service** - Embedded timeline server for file system view caching.
- **packaging/** - Fat jar bundles for deployment (spark-bundle, flink-bundle, utilities-bundle, etc.).

## Key Abstractions

### Data Model
- **HoodieRecord<T>** (`hudi-common`) - A single record with `HoodieKey` (record key + partition path), payload data, and storage location.
- **HoodieKey** - Composite of record key and partition path, uniquely identifies a record.
- **Option<T>** (`hudi-io`) - Hudi's own serializable Optional. `java.util.Optional` is banned by checkstyle.
- **HoodieData<T>** (`hudi-common`) - Engine-agnostic distributed collection abstraction (wraps Spark RDD, Flink list, or plain Java list). Lazy intermediate operations, eager terminal operations.

### Timeline & Transactions
- **HoodieTimeline** - Immutable, chainable representation of table commit history. Actions: COMMIT, DELTA_COMMIT, CLEAN, ROLLBACK, COMPACTION, CLUSTERING, etc. States: REQUESTED -> INFLIGHT -> COMPLETED.
- **HoodieActiveTimeline** - Mutable operations on the active timeline (create/transition instants).
- **HoodieTableMetaClient** - Entry point for accessing table metadata, timeline, and configuration. Built via `HoodieTableMetaClient.Builder`.

### Write Path
`BaseHoodieWriteClient` orchestrates: tag records via index lookup -> partition/bucket -> merge with existing -> write files -> commit to timeline -> optional table services (clean, compact, cluster).

### Read Path
`HoodieFileIndex` (Spark) builds file list from timeline + metadata table -> applies index filters (bloom, column stats, record index) -> `FileSystemViewManager` resolves latest file slices -> engine-specific reader merges base files + log files (for MoR tables).

### Engine Abstraction Pattern
- **HoodieEngineContext** - Abstract execution context with `map()`, `flatMap()`, `reduce()` operations. Implementations: `HoodieSparkEngineContext`, `HoodieFlinkEngineContext`, `HoodieLocalEngineContext`.
- **HoodieTable** - Abstract table with engine-specific implementations (`HoodieSparkTable`, `HoodieFlinkTable`). Generic types `<T, I, K, O>` for payload, input, key, output.

### Storage Layer
- **HoodieStorage** (`hudi-io`) - Abstract filesystem interface (create, open, delete, rename, list). Implementations for Hadoop, S3, GCS.
- **StoragePath** - Engine-independent path abstraction.

### Metadata Table
A hidden Hudi MoR table at `<table>/.hoodie/.metadata` storing file listings, bloom filters, column stats, record-level index, and functional indexes for query acceleration.

## Code Style

Checkstyle is enforced (`style/checkstyle.xml`). Key rules:
- **2-space indentation** for Java (no tabs)
- **Max line length: 200** characters
- **Import order**: `org.apache.hudi` first, then third-party, then `javax`, `java`, `scala` (static imports at bottom)
- **No star imports**, no unused/redundant imports
- **Banned imports**: `java.util.Optional` (use `org.apache.hudi.common.util.Option`), `org.apache.commons.*`, `com.google.common.*` (Hudi has its own utilities), `org.apache.log4j`, `org.codehaus.jackson`
- **No Scala imports in hudi-client-common** (enforced by import-control.xml)
- **JUnit 5 only** (`org.junit.jupiter`). Old JUnit 4 imports are banned except `@Rule` and `runner`.
- Scalastyle enforced for Scala code (`style/scalastyle.xml`)

## CI

- **GitHub Actions** (`.github/workflows/bot.yml`) - Primary CI, runs tests with Spark 3.5 and Flink 1.20 profiles.
- **Azure Pipelines** (`azure-pipelines-20230430.yml`) - Additional CI with Spark 3.5 and Flink 1.18.
- Tests are split into separate CI jobs: Java unit tests, Scala tests (DML vs others), Flink integration tests, etc.
