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

import org.apache.hudi.{DataSourceReadOptions, HoodieDataSourceHelper, HoodieFileIndex, SparkAdapterSupport}
import org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.config.HoodieBootstrapConfig
import org.apache.hudi.util.SparkConfigUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * Scan builder for DSv2 snapshot reads of base files (COW snapshot and MOR
 * read_optimized).
 *
 * Supports column pruning; filter, limit, and aggregate pushdown land in later phases,
 * so [[build]] plans input partitions from the unfiltered latest file slices.
 */
class HoodieScanBuilder(spark: SparkSession,
                        metaClient: HoodieTableMetaClient,
                        tableSchema: StructType,
                        options: Map[String, String]) extends ScanBuilder
  with SupportsPushDownRequiredColumns
  with SparkAdapterSupport {

  private var requiredSchema: StructType = tableSchema

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def build(): Scan = {
    // Invariant established by HoodieV2ReadSupport.isSupportedByDSv2: COW snapshot or
    // MOR read_optimized only, single Parquet base-file format — so reading base files
    // without log merging is correct, and splitting them by byte range is safe.
    val queryType = SparkConfigUtils.getStringWithAltKeys(options, DataSourceReadOptions.QUERY_TYPE)
    require(metaClient.getTableType == COPY_ON_WRITE ||
        queryType == DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL,
      "HoodieScanBuilder supports COW snapshot or read_optimized only; " +
        s"got tableType=${metaClient.getTableType}, queryType=$queryType")

    val fileIndex = HoodieFileIndex(spark, metaClient, Some(tableSchema), options,
      includeLogFiles = false, shouldEmbedFileSlices = false)

    val fullPartSchema = fileIndex.partitionSchema
    val partFieldNames = fullPartSchema.fieldNames.toSet
    // Mirror DSv1's HoodieHadoopFsRelationFactory.shouldExtractPartitionValuesFromPartitionPath:
    // only strip partition columns from the file read schema and supply their values from the
    // parsed partition path when the table was written with drop_partition_columns (the columns
    // are absent from the base files), the user explicitly opted in via
    // EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH, or the bootstrap fast-read path is requested.
    // Otherwise partition columns are persisted in the base files and are read from Parquet —
    // path-derived values would be wrong for timestamp/custom key generators or encoded
    // partition paths. (Bootstrap is currently rejected by HoodieV2ReadSupport; the gate is kept
    // identical for parity.)
    val shouldOmitPartitionColumns =
      metaClient.getTableConfig.shouldDropPartitionColumns && partFieldNames.nonEmpty
    val shouldExtractPartitionValueFromPath =
      options.getOrElse(DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key,
        DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.defaultValue.toString).toBoolean
    val shouldUseBootstrapFastRead =
      options.getOrElse(HoodieBootstrapConfig.DATA_QUERIES_ONLY.key, "false").toBoolean
    val extractPartitionValuesFromPartitionPath =
      shouldOmitPartitionColumns || shouldExtractPartitionValueFromPath || shouldUseBootstrapFastRead

    // When partition values are extracted from the path, the partition column types must come
    // from the file index: TimestampBased/Custom key generators parse partition path values as
    // StringType, so fileIndex.partitionSchema is the only place that reflects what ends up in
    // HoodieInputPartition.partitionValues. Building requiredPartitionSchema from requiredSchema
    // would advertise the original Long/Timestamp type and feed string path values into the
    // Parquet partition reader. If the file-index type differs from the table-schema type,
    // reject — DSv2 cannot reconcile that mismatch without also rewriting
    // HoodieSparkV2Table.schema(), which is out of scope here.
    val (requiredDataSchema, requiredPartitionSchema) = if (extractPartitionValuesFromPartitionPath) {
      val partFieldsByName = fullPartSchema.fields.map(f => f.name -> f).toMap
      val partitionFieldsInRequired = requiredSchema.fields
        .filter(f => partFieldNames.contains(f.name))
        .map { f =>
          val fileIdxField = partFieldsByName(f.name)
          if (fileIdxField.dataType != f.dataType) {
            throw new UnsupportedOperationException(
              s"DSv2 read with extractPartitionValuesFromPartitionPath=true does not support " +
                s"partition column '${f.name}' whose file-index type ${fileIdxField.dataType.simpleString} " +
                s"differs from the table-schema type ${f.dataType.simpleString} (typical for " +
                s"TimestampBasedKeyGenerator). Disable extractPartitionValuesFromPartitionPath, " +
                s"unset hoodie.datasource.write.drop.partition.columns, or use the DSv1 reader.")
          }
          fileIdxField
        }
      (StructType(requiredSchema.filterNot(f => partFieldNames.contains(f.name))),
        StructType(partitionFieldsInRequired))
    } else {
      (requiredSchema, StructType(Nil))
    }

    val hadoopConf = spark.sessionState.newHadoopConf()
    // Row mode: HoodiePartitionReader is a PartitionReader[InternalRow], so the columnar
    // reader must not hand back ColumnarBatches.
    val readerOptions = options + (FileFormat.OPTION_RETURNING_BATCH -> "false")
    val reader = sparkAdapter.createParquetFileReader(false, spark.sessionState.conf, readerOptions, hadoopConf)
    val broadcastReader = spark.sparkContext.broadcast(reader)
    val broadcastConf = spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    // No filters are pushed yet: empty filter lists yield the latest base file of every
    // file slice in scope (the query path may scope the scan to a partition sub-path).
    val fileSlicesPerPartition = fileIndex.filterFileSlices(Seq.empty, Seq.empty)

    // Mirror the DSv1 split path: break each base file into ranges of at most
    // spark.sql.files.maxPartitionBytes so large files don't become single-task
    // stragglers; computeSplitRanges is shared with DSv1 so task boundaries match.
    val partitions = fileSlicesPerPartition.flatMap { case (partitionOpt, fileSlices) =>
      val allPartValues = partitionOpt.map(_.getValues).getOrElse(Array.empty[AnyRef])
      val partValues = if (requiredPartitionSchema.isEmpty) {
        Array.empty[AnyRef]
      } else {
        requiredPartitionSchema.fieldNames.map { name =>
          allPartValues(fullPartSchema.fieldIndex(name))
        }
      }
      fileSlices.filter(_.getBaseFile.isPresent).flatMap { fs =>
        val baseFile = fs.getBaseFile.get()
        HoodieDataSourceHelper.computeSplitRanges(spark, baseFile.getFileSize).map { case (start, length) =>
          HoodieInputPartition(0, baseFile.getPath, start, length, partValues)
        }
      }
    }.zipWithIndex.map { case (p, i) => p.copy(index = i) }.toArray[InputPartition]

    new HoodieBatchScan(
      requiredSchema,
      partitions,
      broadcastReader,
      broadcastConf,
      requiredDataSchema,
      requiredPartitionSchema)
  }
}
