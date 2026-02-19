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

Main Spark datasource module. Aggregates version-independent functionality: SparkSession extension, SQL analysis/resolution rules, DDL/DML command execution, CALL procedures framework, and bootstrap data providers.

## Key Classes

- `HoodieSparkSessionExtension` - Spark session extension injecting Hudi SQL parser and analysis rules
- `HoodieAnalysis` - Catalyst analysis rules for resolving Hudi DDL/DML commands
- `HoodieCommonSqlParser` - Common SQL parser with Hudi-specific grammar extensions
- `CallProcedureHoodieCommand` - Command handler for CALL procedure invocations
- `BaseProcedure` - Base class for all callable Hudi procedures
- `HoodieProcedures` - Registry of 75+ built-in procedures (compaction, clustering, clean, savepoint, etc.)
- `RunCompactionProcedure`, `RunClusteringProcedure`, `ShowCommitsProcedure` - Example stored procedures
- `CompactionHoodieTableCommand` - Compaction scheduling and execution command
- `CreateIndexCommand`, `DropIndexCommand`, `ShowIndexesCommand` - Index management commands
- `SparkParquetBootstrapDataProvider` - Bootstrap data provider for migrating Parquet tables
- `SparkOrcBootstrapDataProvider` - Bootstrap data provider for migrating ORC tables
- `HoodieDataSourceHelpers` - Utility methods for working with Hudi datasource

## Module Dependencies

- `hudi-spark-client` - Spark-specific write client
- `hudi-spark-common` - Cross-version shared Spark integration code
- Version-specific Spark module (selected via Maven profile, e.g., `hudi-spark3.5.x`)
- `hudi-client-common` - Engine-agnostic write client
- `hudi-common` - Core data model and timeline
- `hudi-hadoop-mr` - MapReduce InputFormat for Hudi tables
- `hudi-hive-sync` - Hive metastore synchronization

## Build

This module is built as part of the default Maven build. The version-specific Spark adapter is selected via profile (default: Spark 3.5). It supports both Scala 2.12 and 2.13.
