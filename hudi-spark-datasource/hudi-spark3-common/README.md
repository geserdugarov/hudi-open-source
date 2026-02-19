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

# `hudi-spark3-common` Module

Spark 3.x version-agnostic shared abstraction layer, reused across the Spark 3.3, 3.4, and 3.5 version-specific modules.

## Key Classes

- `BaseSpark3Adapter` - Base Spark 3.x adapter providing version-independent Catalyst, Avro, and schema utilities
- `MergeIntoHoodieTableCommand` - Spark 3.x MERGE INTO DML command implementation
- `InsertIntoHoodieTableCommand` - Spark 3.x INSERT INTO command
- `UpdateHoodieTableCommand` - Spark 3.x UPDATE command
- `DeleteHoodieTableCommand` - Spark 3.x DELETE command
- `CreateHoodieTableAsSelectCommand` - Spark 3.x CTAS command
- `Spark3HoodiePruneFileSourcePartitions` - Partition pruning rule for Spark 3.x
- `Spark3HoodieInternalRow` - Spark 3.x-specific internal row implementation
- `Spark3HoodiePartitionFileSliceMapping` - Partition-to-file-slice mapping for Spark 3.x
- `Spark3HoodieUnsafeUtils` - Unsafe row utilities for Spark 3.x

## Module Dependencies

- `hudi-spark-client` - Spark-specific write client
- `hudi-spark-common` - Cross-version shared Spark integration code

## Build

This module is built as part of the default Maven build when any Spark 3.x profile is active. It supports both Scala 2.12 and 2.13.
