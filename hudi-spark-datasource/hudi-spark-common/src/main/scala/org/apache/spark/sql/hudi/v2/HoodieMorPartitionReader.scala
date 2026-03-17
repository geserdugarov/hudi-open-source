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

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.SparkFileFormatInternalRowReaderContext
import org.apache.hudi.common.config.{HoodieReaderConfig, TypedProperties}
import org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE
import org.apache.hudi.common.model.HoodieBaseFile
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.read.HoodieFileGroupReader
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.common.util.collection.ClosableIterator
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.HoodieCatalystExpressionUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.io.Closeable

import scala.collection.JavaConverters._

/**
 * Partition reader for MoR file slices with log files.
 * Uses [[HoodieFileGroupReader]] to merge base file + log files, following the same pattern
 * as [[org.apache.hudi.HoodieMergeOnReadRDDV2]].
 */
class HoodieMorPartitionReader(partition: HoodieInputPartition,
                                broadcastReader: Broadcast[SparkColumnarFileReader],
                                broadcastConf: Broadcast[SerializableConfiguration],
                                readSchema: StructType,
                                requiredDataSchema: StructType,
                                requiredPartitionSchema: StructType,
                                morContext: MorContext)
  extends PartitionReader[InternalRow] with SparkAdapterSupport {

  private val iter: Iterator[InternalRow] = createIterator()
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
    iter match {
      case c: Closeable => c.close()
      case _ =>
    }
  }

  private def createIterator(): Iterator[InternalRow] = {
    val hadoopConf = broadcastConf.value.value
    val storageConf = new HadoopStorageConfiguration(hadoopConf)

    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(morContext.basePath)
      .setConf(storageConf)
      .build()

    val properties = TypedProperties.fromMap(morContext.options.asJava)
    val maxCompactionMemoryInBytes = getMaxCompactionMemoryInBytes(new JobConf(hadoopConf))
    properties.setProperty(MAX_MEMORY_FOR_MERGE.key(), String.valueOf(maxCompactionMemoryInBytes))
    properties.setProperty(HoodieReaderConfig.MERGE_TYPE.key(), morContext.mergeType)

    val baseFileOption: HOption[HoodieBaseFile] = if (partition.baseFilePath.nonEmpty) {
      HOption.of(new HoodieBaseFile(partition.baseFilePath))
    } else {
      HOption.empty()
    }

    val readerContext = new SparkFileFormatInternalRowReaderContext(
      broadcastReader.value, Seq.empty, Seq.empty, storageConf, metaClient.getTableConfig)

    val fileGroupReader = HoodieFileGroupReader.newBuilder()
      .withReaderContext(readerContext)
      .withHoodieTableMetaClient(metaClient)
      .withLatestCommitTime(morContext.latestCommitTimestamp)
      .withLogFiles(partition.logFiles.asJava.stream())
      .withBaseFileOption(baseFileOption)
      .withPartitionPath(partition.partitionPath)
      .withProps(properties)
      .withDataSchema(morContext.tableDataSchema)
      .withRequestedSchema(morContext.requiredMergeSchema)
      .withInternalSchema(HOption.empty())
      .build()

    val closableIter: ClosableIterator[InternalRow] = fileGroupReader.getClosableIterator
    val mergedIter: Iterator[InternalRow] with Closeable = new Iterator[InternalRow] with Closeable {
      override def hasNext: Boolean = closableIter.hasNext
      override def next(): InternalRow = closableIter.next()
      override def close(): Unit = closableIter.close()
    }

    val mergeStructType = morContext.mergeStructType
    val partValues = InternalRow.fromSeq(partition.partitionValues.toSeq)

    // Project from merge schema to required data schema if mandatory fields were added
    val dataIter = if (mergeStructType != requiredDataSchema) {
      val dataProjection = HoodieCatalystExpressionUtils.generateUnsafeProjection(mergeStructType, requiredDataSchema)
      new Iterator[InternalRow] with Closeable {
        override def hasNext: Boolean = mergedIter.hasNext
        override def next(): InternalRow = dataProjection(mergedIter.next())
        override def close(): Unit = mergedIter.close()
      }
    } else {
      mergedIter
    }

    // Append partition values and project to final readSchema (same pattern as HoodiePartitionReader)
    val readerOutputSchema = StructType(requiredDataSchema.fields ++ requiredPartitionSchema.fields)

    val withPartitions = if (requiredPartitionSchema.isEmpty) {
      dataIter
    } else {
      new Iterator[InternalRow] with Closeable {
        override def hasNext: Boolean = dataIter.hasNext
        override def next(): InternalRow = {
          val dataRow = dataIter.next()
          val joined = new Array[Any](requiredDataSchema.length + requiredPartitionSchema.length)
          (0 until requiredDataSchema.length).foreach(i => joined(i) = dataRow.get(i, requiredDataSchema(i).dataType))
          (0 until requiredPartitionSchema.length).foreach(i =>
            joined(requiredDataSchema.length + i) = partValues.get(i, requiredPartitionSchema(i).dataType))
          InternalRow.fromSeq(joined.toSeq)
        }
        override def close(): Unit = dataIter match {
          case c: Closeable => c.close()
          case _ =>
        }
      }
    }

    if (readerOutputSchema != readSchema) {
      val projection = HoodieCatalystExpressionUtils.generateUnsafeProjection(readerOutputSchema, readSchema)
      new Iterator[InternalRow] with Closeable {
        override def hasNext: Boolean = withPartitions.hasNext
        override def next(): InternalRow = projection(withPartitions.next())
        override def close(): Unit = withPartitions match {
          case c: Closeable => c.close()
          case _ =>
        }
      }
    } else {
      withPartitions
    }
  }
}
