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

# `hudi-spark4-common` Module

Spark 4.x version-agnostic shared abstraction layer, parallel to `hudi-spark3-common`. Contains DML commands and Catalyst utilities adapted for the Spark 4.x API.

## Key Classes

- `BaseSpark4Adapter` - Base Spark 4.x adapter providing version-independent Catalyst, Avro, and schema utilities
- `MergeIntoHoodieTableCommand` - Spark 4.x MERGE INTO DML command implementation
- `InsertIntoHoodieTableCommand` - Spark 4.x INSERT INTO command
- `UpdateHoodieTableCommand` - Spark 4.x UPDATE command
- `DeleteHoodieTableCommand` - Spark 4.x DELETE command
- `CreateHoodieTableAsSelectCommand` - Spark 4.x CTAS command
- `HoodieSpark4CatalystExpressionUtils` - Catalyst expression utilities for Spark 4.x
- `ExpressionColumnNodeWrapper` - Wrapper for Spark 4.x column node expressions
- `Spark4HoodiePruneFileSourcePartitions` - Partition pruning rule for Spark 4.x
- `Spark4HoodieInternalRow` - Spark 4.x-specific internal row implementation

## Module Dependencies

- `hudi-spark-client` - Spark-specific write client
- `hudi-spark-common` - Cross-version shared Spark integration code

## Build

This module is built when the Spark 4.0 profile is active (`-Dspark4.0`). Tests are controlled by the `skip.hudi-spark4.unit.tests` property.
