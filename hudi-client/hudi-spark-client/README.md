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

# `hudi-spark-client` Module

Spark-specific write client and table operations. Provides `SparkRDDWriteClient` for insert, upsert, delete, bulk insert, compaction, and clustering using Spark RDDs.

## Key Classes

- `SparkRDDWriteClient` - Spark-specific write client orchestrating RDD-based write operations
- `HoodieSparkTable` - Abstract Spark table representation with engine-specific write logic
- `HoodieSparkCopyOnWriteTable` - Copy-on-write table implementation for Spark
- `HoodieSparkMergeOnReadTable` - Merge-on-read table implementation extending CoW with compaction support
- `HoodieSparkEngineContext` - Spark execution context implementing `HoodieEngineContext`
- `SimpleKeyGenerator`, `ComplexKeyGenerator`, `CustomKeyGenerator`, `TimestampBasedKeyGenerator` - Key generators for partitioning and record key extraction
- `HoodieSparkKeyGeneratorFactory` - Factory for instantiating key generators
- `SparkHoodieBloomIndexHelper` - Bloom filter indexing utilities for Spark
- `SparkMetadataTableRecordLevelIndex` - Record-level index via the metadata table
- `HoodieSparkConsistentBucketIndex` - Consistent bucket indexing for Spark
- `GlobalSortPartitioner`, `NonSortPartitioner`, `RDDCustomColumnsSortPartitioner` - Bulk insert partitioners with various sorting strategies
- `SparkHoodieBackedTableMetadataWriter` - Metadata table writer for Spark
- `SparkAsyncCompactService`, `SparkAsyncClusteringService` - Async table services
- `SparkUpgradeDowngradeHelper` - Table format upgrade/downgrade support

## Module Dependencies

- `hudi-client-common` - Engine-agnostic write client and table operations
- `hudi-io` - Storage abstraction layer (shaded)
- `hudi-hadoop-common` - Hadoop filesystem integration

## Build

This module is built as part of the default Maven build. It supports both Scala 2.12 and 2.13 via the `scala.binary.version` property. No special profiles are needed.
