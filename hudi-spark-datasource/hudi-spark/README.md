<!--
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
-->

# `hudi-spark` Module

This is the main Spark datasource module for Apache Hudi. It provides Spark Session extensions, the SQL parser for Hudi-specific commands, stored procedures for table management, and index command implementations.

## Overview

The `hudi-spark` module serves as the primary integration point for Hudi with Spark SQL. It implements:

- Spark Session extensions for injecting Hudi-specific parsing and analysis rules
- ANTLR4-based SQL parser for CALL and COMPACTION statements
- 72+ stored procedures for table management, compaction, clustering, metadata, and repair operations
- Index DDL commands (CREATE/DROP/REFRESH/SHOW INDEX)
- Deduplication utilities for Spark jobs

## Directory Structure

```
src/main/
├── antlr4/
│   └── org/apache/hudi/spark/sql/parser/
│       └── HoodieSqlCommon.g4               # ANTLR4 grammar for Hudi SQL
├── java/
│   └── org/apache/hudi/
│       ├── HoodieDataSourceHelpers.java     # Timeline query helpers
│       ├── QuickstartUtils.java             # Demo/quickstart utilities
│       ├── bootstrap/                        # Bootstrap data providers
│       │   ├── SparkFullBootstrapDataProviderBase.java
│       │   ├── SparkParquetBootstrapDataProvider.java
│       │   └── SparkOrcBootstrapDataProvider.java
│       └── cli/                              # CLI utilities
│           ├── ArchiveExecutorUtils.java
│           ├── BootstrapExecutorUtils.java
│           ├── HDFSParquetImporterUtils.java
│           └── SchemaProvider.java
└── scala/
    └── org/apache/spark/sql/
        ├── catalyst/plans/logical/
        │   ├── Call.scala                   # CALL statement logical plan
        │   └── Compaction.scala             # Compaction logical plans
        ├── hudi/
        │   ├── HoodieSparkSessionExtension.scala
        │   ├── SparkHelpers.scala
        │   ├── DedupeSparkJob.scala
        │   ├── DeDupeType.scala
        │   ├── analysis/
        │   │   └── HoodieAnalysis.scala
        │   └── command/
        │       ├── CallProcedureHoodieCommand.scala
        │       ├── CompactionHoodieTableCommand.scala
        │       ├── CompactionHoodiePathCommand.scala
        │       ├── CompactionShowHoodieTableCommand.scala
        │       ├── CompactionShowHoodiePathCommand.scala
        │       ├── IndexCommands.scala
        │       ├── UuidKeyGenerator.scala
        │       └── procedures/              # 72+ stored procedures
        └── parser/
            ├── HoodieCommonSqlParser.scala
            └── HoodieSqlCommonAstBuilder.scala
```

## Key Components

### Spark Session Extension

| Class | Description |
|-------|-------------|
| **HoodieSparkSessionExtension** | Entry point for Spark session extensions. Injects custom SQL parser (`HoodieCommonSqlParser`), resolution rules, post-hoc resolution rules, and optimizer rules via `SparkSessionExtensions`. Register using `spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension`. |
| **HoodieAnalysis** | Central analysis rule orchestrator. Registers custom resolution rules (`DataSourceV2ToV1Fallback`, `ResolveHudiAlterTableCommand`), post-hoc resolution rules, and optimizer rules for Hudi-specific SQL transformations. |

### SQL Parser

| Class | Description |
|-------|-------------|
| **HoodieCommonSqlParser** | Main SQL parser interface wrapping Spark's `ParserInterface`. Delegates Hudi-specific syntax (CALL, COMPACTION) to `HoodieSqlCommonAstBuilder` while passing through standard SQL to the underlying Spark parser. |
| **HoodieSqlCommonAstBuilder** | ANTLR4 AST visitor that converts parse trees to `LogicalPlan` objects. Handles CALL statements, COMPACTION commands (RUN/SCHEDULE/SHOW), and table/path identifiers. |
| **HoodieSqlCommon.g4** | ANTLR4 grammar defining Hudi SQL syntax including CALL procedure arguments, COMPACTION operations, MAP literals, and expression handling. |

### Logical Plans

| Class | Description |
|-------|-------------|
| **Call** | Logical plan for CALL procedure statements. Contains procedure name and argument list. |
| **Compaction** | Logical plans for compaction operations (run, schedule, show) with table/path identifiers. |

### SQL Commands

| Class | Description |
|-------|-------------|
| **CallProcedureHoodieCommand** | Executes CALL procedure statements by looking up the procedure from `HoodieProcedures` registry and invoking it with provided arguments. |
| **CompactionHoodieTableCommand** | Runs compaction on a Hudi table identified by table name. |
| **CompactionHoodiePathCommand** | Runs compaction on a Hudi table identified by file path. |
| **CompactionShowHoodieTableCommand** | Shows compaction status for a table. |
| **CompactionShowHoodiePathCommand** | Shows compaction status for a file path. |
| **IndexCommands** | Implements CREATE INDEX, DROP INDEX, REFRESH INDEX, and SHOW INDEXES commands for Hudi secondary indexes. |

### Utilities

| Class | Description |
|-------|-------------|
| **SparkHelpers** | Utility functions for file operations, including `skipKeysAndWriteNewFile()` for reading/writing parquet files with Hudi schemas and bloom filters. |
| **DedupeSparkJob** | Deduplication logic for Spark jobs. |
| **DeDupeType** | Enumeration for deduplication types (INSERT_TYPE, UPDATE_TYPE, UPSERT_TYPE). |
| **UuidKeyGenerator** | UUID key generation utility. |

## Stored Procedures

The module includes 72+ stored procedures organized by category:

### Compaction & Clustering

| Procedure | Description |
|-----------|-------------|
| `run_compaction` | Execute/schedule compaction on a table |
| `show_compaction` | Display compaction status |
| `run_clustering` | Execute clustering operations |
| `show_clustering` | Display clustering status |

### Savepoint Operations

| Procedure | Description |
|-----------|-------------|
| `create_savepoint` | Create a table savepoint |
| `delete_savepoint` | Delete a savepoint |
| `show_savepoints` | List all savepoints |
| `rollback_to_savepoint` | Rollback to a specific savepoint |

### Rollback & Timeline

| Procedure | Description |
|-----------|-------------|
| `rollback_to_instant_time` | Rollback to a specific instant time |
| `show_rollbacks` | Display rollback operations |
| `run_rollback_inflight_table_service` | Rollback inflight table services |
| `export_instants` | Export timeline instants |

### Commits & Clean

| Procedure | Description |
|-----------|-------------|
| `show_commits` | Display commits with optional extra metadata |
| `show_archived_commits` | Display archived commits |
| `show_commit_files` | Files changed in a commit |
| `show_commit_partitions` | Partitions modified in a commit |
| `show_commit_write_stats` | Write statistics for commits |
| `commits_compare` | Compare commits |
| `run_clean` | Execute table cleanup |
| `show_cleans` | Display clean operations |
| `archive_commits` | Archive old commits |

### Metadata Table

| Procedure | Description |
|-----------|-------------|
| `create_metadata_table` | Create metadata table |
| `init_metadata_table` | Initialize metadata table |
| `delete_metadata_table` | Delete metadata table |
| `show_metadata_table_stats` | Metadata table statistics |
| `show_metadata_table_column_stats` | Column statistics from metadata |
| `show_metadata_table_files` | Files in metadata table |
| `show_metadata_table_partitions` | Partitions in metadata table |
| `validate_metadata_table_files` | Validate metadata table file listings |

### File System & Log

| Procedure | Description |
|-----------|-------------|
| `show_file_system_view` | Display file system view |
| `show_file_status` | File status information |
| `show_fs_path_detail` | Detailed path information |
| `show_hoodie_log_file_metadata` | Log file metadata |
| `show_hoodie_log_file_records` | Log file records |
| `show_invalid_parquet` | Invalid parquet files |
| `delete_marker` | Delete write markers |

### Statistics

| Procedure | Description |
|-----------|-------------|
| `stats_file_size` | File size statistics |
| `stats_write_amplification` | Write amplification metrics |

### Bootstrap & Import

| Procedure | Description |
|-----------|-------------|
| `run_bootstrap` | Bootstrap table from external data |
| `show_bootstrap_mapping` | Bootstrap mapping information |
| `show_bootstrap_partitions` | Bootstrap partition information |
| `hdfs_parquet_import` | Import parquet from HDFS |

### Repair & Maintenance

| Procedure | Description |
|-----------|-------------|
| `repair_add_partition_meta` | Fix partition metadata |
| `repair_corrupted_clean_files` | Fix corrupted clean files |
| `repair_deduplicate` | Remove duplicate records |
| `repair_migrate_partition_meta` | Migrate partition metadata |
| `repair_overwrite_hoodie_props` | Overwrite table properties |

### Table Operations

| Procedure | Description |
|-----------|-------------|
| `upgrade_or_downgrade_table` | Upgrade/downgrade table format |
| `truncate_table` | Truncate all table data |
| `drop_partition` | Drop table partition |
| `run_ttl` | Execute Time-To-Live cleanup |
| `show_table_properties` | Display table properties |
| `show_timeline` | Display complete timeline |

### Sync & Copy

| Procedure | Description |
|-----------|-------------|
| `copy_to_table` | Copy to target table |
| `copy_to_temp_view` | Copy to temporary view |
| `hive_sync` | Sync with Hive metastore |
| `validate_hoodie_sync` | Validate Hive sync |
| `help` | Help/documentation for procedures |

## Procedure Infrastructure

| Class | Description |
|-------|-------------|
| **Procedure** | Trait defining the procedure interface (parameters, outputType, call, description). |
| **BaseProcedure** | Abstract base class with SparkSession access, write config creation, parameter validation. |
| **ProcedureBuilder** | Trait for building procedure instances. |
| **HoodieProcedures** | Registry/factory maintaining map of all available procedures via `initProcedureBuilders`. |
| **ProcedureArgs** | Wrapper for procedure arguments. |
| **ProcedureParameter** | Trait defining procedure parameter metadata. |

## Java Classes

| Class | Description |
|-------|-------------|
| **HoodieDataSourceHelpers** | Timeline query utilities: `hasNewCommits()`, `listCommitsSince()`, incremental view helpers. |
| **QuickstartUtils** | Quickstart and demo utilities. |
| **SparkFullBootstrapDataProviderBase** | Abstract base for bootstrap data providers. |
| **SparkParquetBootstrapDataProvider** | Bootstrap from parquet files. |
| **SparkOrcBootstrapDataProvider** | Bootstrap from ORC files. |
| **ArchiveExecutorUtils** | Archive operation utilities. |
| **BootstrapExecutorUtils** | Bootstrap execution helpers. |
| **HDFSParquetImporterUtils** | Import parquet utilities. |

## Usage

### Registering Hudi Extensions

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("HudiExample")
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
  .getOrCreate()
```

### Calling Procedures

```sql
-- Run compaction
CALL run_compaction(table => 'my_table', op => 'RUN');

-- Show commits
CALL show_commits(table => 'my_table', limit => 10);

-- Create savepoint
CALL create_savepoint(table => 'my_table', commit_time => '20230601120000');

-- Show metadata table stats
CALL show_metadata_table_stats(table => 'my_table');

-- Run clustering
CALL run_clustering(table => 'my_table');
```

### Compaction Commands

```sql
-- Run compaction on table
RUN COMPACTION ON my_table;

-- Schedule compaction
SCHEDULE COMPACTION ON my_table;

-- Show compaction status
SHOW COMPACTION ON my_table LIMIT 10;
```

### Index Commands

```sql
-- Create secondary index
CREATE INDEX idx_name ON my_table (column_name);

-- Show indexes
SHOW INDEXES ON my_table;

-- Refresh index
REFRESH INDEX idx_name ON my_table;

-- Drop index
DROP INDEX idx_name ON my_table;
```

## Dependencies

- `hudi-spark-common` - Core shared Spark integration
- `hudi-spark3-common` or `hudi-spark4-common` - Version-specific code
- Apache Spark SQL APIs
- ANTLR4 runtime for SQL parsing

## Build Instructions

```bash
# Build with default Spark version (3.5)
mvn clean package -DskipTests -pl hudi-spark-datasource/hudi-spark -am

# Build with specific Spark versions
mvn clean package -DskipTests -Dspark3.3 -pl hudi-spark-datasource/hudi-spark -am
mvn clean package -DskipTests -Dspark3.4 -pl hudi-spark-datasource/hudi-spark -am
mvn clean package -DskipTests -Dspark3.5 -pl hudi-spark-datasource/hudi-spark -am
mvn clean package -DskipTests -Dspark4.0 -pl hudi-spark-datasource/hudi-spark -am
```

## Running Tests

```bash
# Unit tests
mvn test -Punit-tests -pl hudi-spark-datasource/hudi-spark -am

# Run specific Scala test suites
mvn test -Punit-tests -pl hudi-spark-datasource/hudi-spark \
    -DwildcardSuites=org.apache.spark.sql.hudi.dml

# Skip Java tests when running only Scala tests
mvn test -Punit-tests -pl hudi-spark-datasource/hudi-spark \
    -Dtest=skipJavaTests -DwildcardSuites=org.apache.spark.sql.hudi.dml
```

## Related Modules

- [hudi-spark-datasource](../README.md) - Parent module overview
- [hudi-spark-common](../hudi-spark-common/README.md) - Core shared code
- [hudi-spark3-common](../hudi-spark3-common/README.md) - Spark 3.x shared code
- [hudi-spark4-common](../hudi-spark4-common/README.md) - Spark 4.x shared code
