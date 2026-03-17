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

import org.apache.hudi.HoodieTableSchema
import org.apache.hudi.cdc.{CDCFileGroupIterator, HoodieCDCFileIndex}
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._

/**
 * Partition reader for CDC queries. Wraps [[CDCFileGroupIterator]] to produce CDC records.
 */
class HoodieCdcPartitionReader(partition: HoodieCdcInputPartition,
                                broadcastReader: Broadcast[SparkColumnarFileReader],
                                broadcastConf: Broadcast[SerializableConfiguration],
                                basePath: String,
                                originTableSchema: HoodieTableSchema,
                                requiredCdcSchema: StructType,
                                options: Map[String, String])
  extends PartitionReader[InternalRow] {

  private val iter: CDCFileGroupIterator = createIterator()
  private var current: InternalRow = _

  override def next(): Boolean = {
    if (iter.hasNext) {
      current = iter.next()
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = current

  override def close(): Unit = {
    iter.close()
  }

  private def createIterator(): CDCFileGroupIterator = {
    val hadoopConf = broadcastConf.value.value
    val storageConf = new HadoopStorageConfiguration(hadoopConf)

    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(storageConf)
      .build()

    val props = TypedProperties.fromMap(options.asJava)

    new CDCFileGroupIterator(
      partition.cdcFileGroupSplit,
      metaClient,
      storageConf,
      broadcastReader.value,
      originTableSchema,
      HoodieCDCFileIndex.FULL_CDC_SPARK_SCHEMA,
      requiredCdcSchema,
      props)
  }
}
