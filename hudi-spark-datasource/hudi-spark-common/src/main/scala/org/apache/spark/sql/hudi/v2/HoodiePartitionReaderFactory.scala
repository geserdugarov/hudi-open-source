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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * Factory that creates partition readers for DSv2 snapshot reads.
 * Routes to [[HoodiePartitionReader]] for CoW / base-only slices,
 * and [[HoodieMorPartitionReader]] for slices with log files.
 */
class HoodiePartitionReaderFactory(broadcastReader: Broadcast[SparkColumnarFileReader],
                                   broadcastConf: Broadcast[SerializableConfiguration],
                                   readSchema: StructType,
                                   requiredDataSchema: StructType,
                                   requiredPartitionSchema: StructType,
                                   morContext: Option[MorContext] = None,
                                   includedCommitTimes: Option[Set[String]] = None,
                                   pushedLimit: Option[Int] = None) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val hoodiePart = partition.asInstanceOf[HoodieInputPartition]
    if (hoodiePart.logFiles.nonEmpty && morContext.isDefined) {
      new HoodieMorPartitionReader(
        hoodiePart,
        broadcastReader,
        broadcastConf,
        readSchema,
        requiredDataSchema,
        requiredPartitionSchema,
        morContext.get,
        includedCommitTimes,
        pushedLimit)
    } else {
      new HoodiePartitionReader(
        hoodiePart,
        broadcastReader,
        broadcastConf,
        readSchema,
        requiredDataSchema,
        requiredPartitionSchema,
        includedCommitTimes,
        pushedLimit)
    }
  }
}
