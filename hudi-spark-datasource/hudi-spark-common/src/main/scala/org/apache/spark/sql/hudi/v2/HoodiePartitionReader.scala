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
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.ParquetTableSchemaResolver
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.MessageType
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.HoodieCatalystExpressionUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.io.Closeable

/**
 * Partition reader that reads its assigned byte range of a single base (Parquet) file
 * using [[SparkColumnarFileReader]] in row mode, honoring the internal schema
 * (schema-on-read evolution) and the table Avro schema (logical-type repair) resolved
 * by [[HoodieScanBuilder]].
 */
class HoodiePartitionReader(partition: HoodieInputPartition,
                            broadcastReader: Broadcast[SparkColumnarFileReader],
                            broadcastConf: Broadcast[SerializableConfiguration],
                            readSchema: StructType,
                            requiredDataSchema: StructType,
                            requiredPartitionSchema: StructType,
                            internalSchemaOpt: HOption[InternalSchema],
                            tableAvroSchema: HOption[HoodieSchema])
  extends PartitionReader[InternalRow] with SparkAdapterSupport {

  private val (rawIter, projectedIter): (Iterator[InternalRow], Iterator[InternalRow]) = createIterators()
  private var current: InternalRow = _

  override def next(): Boolean = {
    if (projectedIter.hasNext) {
      current = projectedIter.next()
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = current

  override def close(): Unit = {
    rawIter match {
      case c: Closeable => c.close()
      case _ =>
    }
  }

  private def createIterators(): (Iterator[InternalRow], Iterator[InternalRow]) = {
    val partValues = InternalRow.fromSeq(partition.partitionValues.toSeq)

    val pFile = sparkAdapter.getSparkPartitionedFileUtils
      .createPartitionedFile(partValues, new StoragePath(partition.baseFilePath),
        partition.start, partition.length)

    val storageConf = new HadoopStorageConfiguration(broadcastConf.value.value)
    // Mirror DSv1 (HoodieFileGroupReaderBasedFileFormat.tableSchemaAsMessageType): convert
    // the table Avro schema to a Parquet MessageType so the reader can repair logical
    // timestamp annotations (timestamp-millis) that older base files lost — without it
    // those columns surface as their physical long value. The fresh Configuration matches
    // DSv1's conversion context.
    val tableSchemaOpt: HOption[MessageType] = if (tableAvroSchema.isPresent) {
      HOption.ofNullable(
        ParquetTableSchemaResolver.convertAvroSchemaToParquet(tableAvroSchema.get(), new Configuration()))
    } else {
      HOption.empty[MessageType]()
    }
    val rawIter = broadcastReader.value.read(
      pFile, requiredDataSchema, requiredPartitionSchema,
      internalSchemaOpt, Seq.empty, storageConf, tableSchemaOpt)

    // The reader emits requiredDataSchema columns with requiredPartitionSchema values
    // appended at the end; realign to readSchema when the two layouts differ (e.g. a
    // projected partition column that is not last in the projection).
    val readerOutputSchema = StructType(requiredDataSchema.fields ++ requiredPartitionSchema.fields)
    val projectedIter = if (readerOutputSchema != readSchema) {
      val projection = HoodieCatalystExpressionUtils.generateUnsafeProjection(readerOutputSchema, readSchema)
      rawIter.map(row => projection(row))
    } else {
      rawIter
    }
    (rawIter, projectedIter)
  }
}
