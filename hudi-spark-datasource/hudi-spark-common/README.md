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

# `hudi-spark-common` Module

Cross-version shared Spark integration code used by both Spark 3.x and 4.x. Contains the core datasource logic: data source registration, read/write relations, file indexes, streaming support, catalog integration, and SQL commands.

## Key Classes

- `DefaultSource` - Spark datasource entry point (`RelationProvider`, `CreatableRelationProvider`, `StreamSinkProvider`)
- `HoodieSparkSqlWriter` - Core write path orchestrating Spark SQL writes to Hudi tables
- `HoodieFileIndex` - File index supporting partition pruning for snapshot and read-optimized queries
- `SparkHoodieTableFileIndex` - Base file index with metadata table and data skipping support
- `HoodieBaseRelation` - Base relation providing schema resolution, file reading, and merge-on-read support
- `MergeOnReadSnapshotRelation` - Relation for MoR snapshot queries merging base files with log files
- `IncrementalRelationV1` - Relation for incremental queries
- `HoodieStreamingSink` - Spark Structured Streaming sink for Hudi
- `HoodieCatalog` - Spark catalog plugin for Hudi table management
- `ColumnStatsIndexSupport` - Column statistics index for data skipping optimizations
- `RecordLevelIndexSupport` - Record-level index support for point lookups
- `DataSourceReadOptions`, `DataSourceWriteOptions` - Configuration options for read and write operations

## Module Dependencies

- `hudi-client-common` - Engine-agnostic write client
- `hudi-spark-client` - Spark-specific write client
- `hudi-common` - Core data model and timeline
- `hudi-hadoop-common` - Hadoop filesystem integration
- `hudi-hive-sync` - Hive metastore synchronization

## Build

This module is built as part of the default Maven build. It supports both Scala 2.12 and 2.13 via the `scala.binary.version` property. No special profiles are needed.
