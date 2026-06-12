/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.v2

import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.internal.schema.InternalSchema

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * Batch scan for snapshot reads of base files via DSv2 (COW snapshot and MOR
 * read_optimized).
 *
 * Holds the input partitions pre-planned by [[HoodieScanBuilder]] (already pruned by the
 * pushed partition filters and metadata-table data skipping) together with the broadcast
 * Parquet reader and hadoop conf the executors read with, plus the internal schema
 * (schema-on-read evolution) and table Avro schema (Parquet logical-type repair) resolved
 * as of the queried instant. `pushedParquetFilters` are handed to the Parquet reader for
 * row-group pruning only — Spark re-applies all filters post-scan. Statistics reporting
 * for CBO lands with a later phase.
 */
class HoodieBatchScan(outputSchema: StructType,
                      inputPartitions: Array[InputPartition],
                      broadcastReader: Broadcast[SparkColumnarFileReader],
                      broadcastConf: Broadcast[SerializableConfiguration],
                      requiredDataSchema: StructType,
                      requiredPartitionSchema: StructType,
                      internalSchemaOpt: HOption[InternalSchema],
                      tableAvroSchema: HOption[HoodieSchema],
                      val pushedFilters: Array[Filter],
                      val pushedParquetFilters: Array[Filter]) extends Scan with Batch {

  override def readSchema(): StructType = outputSchema

  override def description(): String =
    s"HoodieBatchScan ${outputSchema.catalogString}, PushedFilters: [${pushedFilters.mkString(", ")}]"

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = inputPartitions

  override def createReaderFactory(): PartitionReaderFactory = {
    new HoodiePartitionReaderFactory(
      broadcastReader,
      broadcastConf,
      outputSchema,
      requiredDataSchema,
      requiredPartitionSchema,
      internalSchemaOpt,
      tableAvroSchema,
      pushedParquetFilters)
  }
}
