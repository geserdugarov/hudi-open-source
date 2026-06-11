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

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

/**
 * Batch scan for snapshot reads via DSv2.
 *
 * Skeleton: holds pre-planned input partitions (currently always empty) and creates the
 * [[HoodiePartitionReaderFactory]]. Reader broadcasting and statistics reporting land
 * with the COW snapshot read phase.
 */
class HoodieBatchScan(outputSchema: StructType,
                      inputPartitions: Array[InputPartition]) extends Scan with Batch {

  override def readSchema(): StructType = outputSchema

  override def description(): String = s"HoodieBatchScan ${outputSchema.catalogString}"

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = inputPartitions

  override def createReaderFactory(): PartitionReaderFactory = new HoodiePartitionReaderFactory()
}
