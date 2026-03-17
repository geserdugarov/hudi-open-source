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

import org.apache.hudi.{BaseHoodieTableFileIndex, DataSourceReadOptions, HoodieFileIndex, HoodieSchemaConversionUtils, HoodieTableSchema, SparkAdapterSupport}
import org.apache.hudi.HoodieBaseRelation.convertToHoodieSchema
import org.apache.hudi.cdc.{HoodieCDCFileGroupSplit, HoodieCDCFileIndex}
import org.apache.hudi.common.config.HoodieReaderConfig
import org.apache.hudi.common.model.{HoodieLogFile, HoodieTableType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.common.table.cdc.HoodieCDCExtractor
import org.apache.hudi.common.table.log.InstantRange
import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer
import org.apache.hudi.common.table.timeline.TimelineUtils.getCommitMetadata
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.listAffectedFilesForCommits
import org.apache.hudi.metadata.HoodieTableMetadataUtil.getWritePartitionPaths

import org.apache.spark.sql.HoodieCatalystExpressionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._

/**
 * Scan builder for DSv2 reads. Supports snapshot, read_optimized, incremental, and CDC queries.
 */
class HoodieScanBuilder(spark: SparkSession,
                        metaClient: HoodieTableMetaClient,
                        tableSchema: StructType,
                        options: Map[String, String]) extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns
  with SparkAdapterSupport {

  private val queryType = options.getOrElse(
    DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
  private val incrementalFormat = options.getOrElse(
    DataSourceReadOptions.INCREMENTAL_FORMAT.key, DataSourceReadOptions.INCREMENTAL_FORMAT_LATEST_STATE_VAL)
  private val isCdcQuery = queryType == DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL &&
    incrementalFormat == DataSourceReadOptions.INCREMENTAL_FORMAT_CDC_VAL
  private val isIncrementalQuery = queryType == DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL && !isCdcQuery
  private val isReadOptimized = queryType == DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL

  private val effectiveTableSchema: StructType =
    if (isCdcQuery) HoodieCDCFileIndex.FULL_CDC_SPARK_SCHEMA else tableSchema

  private var requiredSchema: StructType = effectiveTableSchema
  private var _pushedFilters: Array[Filter] = Array.empty
  private var partitionFilterExprs: Seq[Expression] = Seq.empty
  private var dataFilterExprs: Seq[Expression] = Seq.empty

  private val isMoR: Boolean =
    metaClient.getTableConfig.getTableType == HoodieTableType.MERGE_ON_READ

  private val includeLogFiles: Boolean = isMoR && !isReadOptimized

  private lazy val fileIndex = HoodieFileIndex(spark, metaClient, None, options,
    includeLogFiles = includeLogFiles, shouldEmbedFileSlices = false)

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (isCdcQuery) {
      // CDC has its own schema; don't push filters
      _pushedFilters = Array.empty
      filters
    } else {
      val (pushed, postScan) = filters.partition { f =>
        HoodieCatalystExpressionUtils.convertToCatalystExpression(f, effectiveTableSchema).isDefined
      }

      val expressions = pushed.flatMap(f =>
        HoodieCatalystExpressionUtils.convertToCatalystExpression(f, effectiveTableSchema))

      val (partFilters, datFilters) = HoodieCatalystExpressionUtils
        .splitPartitionAndDataPredicates(spark, expressions, fileIndex.partitionSchema.fieldNames)

      partitionFilterExprs = partFilters.toSeq
      dataFilterExprs = datFilters.toSeq

      _pushedFilters = pushed

      val partFieldNames = fileIndex.partitionSchema.fieldNames.toSet
      val dataFilterArr = pushed.filterNot(f => f.references.forall(partFieldNames.contains))
      postScan ++ dataFilterArr
    }
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def build(): Scan = {
    if (isCdcQuery) buildCdcScan()
    else if (isIncrementalQuery) buildIncrementalScan()
    else buildSnapshotScan()
  }

  private def buildSnapshotScan(): Scan = {
    val partFieldNames = fileIndex.partitionSchema.fieldNames.toSet
    val requiredDataSchema = StructType(requiredSchema.filterNot(f => partFieldNames.contains(f.name)))
    val requiredPartitionSchema = StructType(requiredSchema.filter(f => partFieldNames.contains(f.name)))

    val hadoopConf = spark.sessionState.newHadoopConf()
    val readerOptions = options + (FileFormat.OPTION_RETURNING_BATCH -> "false")
    val reader = sparkAdapter.createParquetFileReader(false, spark.sessionState.conf, readerOptions, hadoopConf)
    val broadcastReader = spark.sparkContext.broadcast(reader)
    val broadcastConf = spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val fullPartSchema = fileIndex.partitionSchema
    val fileSlicesPerPartition = fileIndex.filterFileSlices(dataFilterExprs, partitionFilterExprs)

    val partitions = buildPartitions(fileSlicesPerPartition, fullPartSchema, requiredPartitionSchema)

    val morCtx = buildMorContext(requiredDataSchema)

    new HoodieBatchScan(
      requiredSchema,
      partitions,
      broadcastReader,
      broadcastConf,
      requiredDataSchema,
      requiredPartitionSchema,
      _pushedFilters,
      morCtx)
  }

  private def buildIncrementalScan(): Scan = {
    if (!options.contains(DataSourceReadOptions.START_COMMIT.key)) {
      throw new HoodieException(s"Specify the begin instant time to pull from using " +
        s"option ${DataSourceReadOptions.START_COMMIT.key}")
    }

    if (!metaClient.getTableConfig.populateMetaFields()) {
      throw new HoodieException("Incremental queries are not supported when meta fields are disabled")
    }

    val partFieldNames = fileIndex.partitionSchema.fieldNames.toSet

    val hadoopConf = spark.sessionState.newHadoopConf()
    val readerOptions = options + (FileFormat.OPTION_RETURNING_BATCH -> "false")
    val reader = sparkAdapter.createParquetFileReader(false, spark.sessionState.conf, readerOptions, hadoopConf)
    val broadcastReader = spark.sparkContext.broadcast(reader)
    val broadcastConf = spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    // Use IncrementalQueryAnalyzer to determine included commits
    val queryContext = IncrementalQueryAnalyzer.builder()
      .metaClient(metaClient)
      .startCompletionTime(options(DataSourceReadOptions.START_COMMIT.key))
      .endCompletionTime(options.getOrElse(DataSourceReadOptions.END_COMMIT.key, null))
      .skipCompaction(options.getOrElse(DataSourceReadOptions.INCREMENTAL_READ_SKIP_COMPACT.key(),
        String.valueOf(DataSourceReadOptions.INCREMENTAL_READ_SKIP_COMPACT.defaultValue)).toBoolean)
      .rangeType(InstantRange.RangeType.OPEN_CLOSED)
      .build()
      .analyze()

    val requiredDataSchema = StructType(requiredSchema.filterNot(f => partFieldNames.contains(f.name)))
    val requiredPartitionSchema = StructType(requiredSchema.filter(f => partFieldNames.contains(f.name)))

    if (queryContext.isEmpty) {
      new HoodieBatchScan(
        requiredSchema,
        Array.empty[InputPartition],
        broadcastReader,
        broadcastConf,
        requiredDataSchema,
        requiredPartitionSchema,
        _pushedFilters)
    } else {
      val includedInstants = queryContext.getInstants.asScala.toList
      val includedCommitTimes = includedInstants.map(_.requestedTime).toSet
      val latestCommit = queryContext.getLastInstant

      // Get commit metadata from the appropriate timeline
      val commitsMetadata = includedInstants.map { i =>
        if (queryContext.getArchivedInstants.contains(i)) {
          getCommitMetadata(i, queryContext.getArchivedTimeline)
        } else {
          getCommitMetadata(i, queryContext.getActiveTimeline)
        }
      }.asJava

      val affectedFilesInCommits = listAffectedFilesForCommits(
        hadoopConf, metaClient.getBasePath, commitsMetadata)

      val modifiedPartitions = getWritePartitionPaths(commitsMetadata)

      // Apply partition filters to narrow modified partitions
      val filteredModifiedPartitions = if (partitionFilterExprs.nonEmpty) {
        val matchingPaths = fileIndex.listMatchingPartitionPaths(
          org.apache.hudi.HoodieFileIndex.convertFilterForTimestampKeyGenerator(metaClient, partitionFilterExprs))
          .map(_.getPath).toSet
        modifiedPartitions.asScala.filter(matchingPaths.contains).toSeq
      } else {
        modifiedPartitions.asScala.toSeq
      }

      // Build file system view and get file slices
      val timeline = queryContext.getActiveTimeline
      val fsView = new HoodieTableFileSystemView(metaClient, timeline, affectedFilesInCommits)

      val fullPartSchema = fileIndex.partitionSchema
      // Build partition path -> partition values lookup from fileIndex
      val partPathToValues: Map[String, Array[AnyRef]] = {
        val allPartitions = fileIndex.filterFileSlices(Seq.empty, partitionFilterExprs)
        allPartitions.flatMap { case (partOpt, _) =>
          partOpt.map(p => p.getPath -> p.getValues)
        }.toMap
      }

      val fileSlices = filteredModifiedPartitions.flatMap { partition =>
        fsView.getLatestMergedFileSlicesBeforeOrOn(partition, latestCommit).iterator().asScala
      }

      val partitions = fileSlices.filter(fs => fs.getBaseFile.isPresent || fs.hasLogFiles).map { fs =>
        val baseFilePath = if (fs.getBaseFile.isPresent) fs.getBaseFile.get().getPath else ""
        val baseFileLength = if (fs.getBaseFile.isPresent) fs.getBaseFile.get().getFileSize else 0L

        val relPartPath = fs.getPartitionPath
        val allPartValues = partPathToValues.getOrElse(relPartPath, Array.empty[AnyRef])

        val partValues = if (requiredPartitionSchema.isEmpty) {
          Array.empty[AnyRef]
        } else {
          requiredPartitionSchema.fieldNames.map { name =>
            val idx = fullPartSchema.fieldIndex(name)
            if (idx < allPartValues.length) allPartValues(idx) else null.asInstanceOf[AnyRef]
          }
        }

        val logFiles = fs.getLogFiles.sorted(HoodieLogFile.getLogFileComparator)
          .iterator().asScala.toList

        HoodieInputPartition(0, baseFilePath, baseFileLength, partValues, logFiles, relPartPath)
      }.zipWithIndex.map { case (p, i) => p.copy(index = i) }.toArray[InputPartition]

      val morCtx = buildMorContext(requiredDataSchema)

      new HoodieBatchScan(
        requiredSchema,
        partitions,
        broadcastReader,
        broadcastConf,
        requiredDataSchema,
        requiredPartitionSchema,
        _pushedFilters,
        morCtx,
        Some(includedCommitTimes))
    }
  }

  private def buildCdcScan(): Scan = {
    if (!options.contains(DataSourceReadOptions.START_COMMIT.key)) {
      throw new HoodieException(s"CDC Query should provide the valid start completion time " +
        s"through the option ${DataSourceReadOptions.START_COMMIT.key}")
    }

    val hadoopConf = spark.sessionState.newHadoopConf()
    val readerOptions = options + (FileFormat.OPTION_RETURNING_BATCH -> "false")
    val reader = sparkAdapter.createParquetFileReader(false, spark.sessionState.conf, readerOptions, hadoopConf)
    val broadcastReader = spark.sparkContext.broadcast(reader)
    val broadcastConf = spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val startCommit = options(DataSourceReadOptions.START_COMMIT.key)
    val endCommit = options.getOrElse(DataSourceReadOptions.END_COMMIT.key, {
      val lastInstant = metaClient.getActiveTimeline.lastInstant()
      if (lastInstant.isPresent) lastInstant.get().requestedTime
      else throw new HoodieException("No valid instant in Active Timeline.")
    })

    val instantRange = InstantRange.builder()
      .startInstant(startCommit)
      .endInstant(endCommit)
      .nullableBoundary(true)
      .rangeType(InstantRange.RangeType.OPEN_CLOSED)
      .build()

    val cdcExtractor = new HoodieCDCExtractor(metaClient, instantRange, false)
    val cdcFileSplits = cdcExtractor.extractCDCFileSplits()

    // Convert Map[HoodieFileGroupId, List[HoodieCDCFileSplit]] to HoodieCdcInputPartition
    val partitions = cdcFileSplits.asScala.zipWithIndex.map { case ((_, fileSplits), idx) =>
      val sortedSplits = fileSplits.asScala.sortBy(_.getInstant).toArray
      HoodieCdcInputPartition(idx, HoodieCDCFileGroupSplit(sortedSplits)): InputPartition
    }.toArray

    // Build the origin table schema needed by CDCFileGroupIterator
    val schemaResolver = new TableSchemaResolver(metaClient)
    val avroSchema = schemaResolver.getTableSchema
    val sparkSchema = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(avroSchema)
    val originTableSchema = HoodieTableSchema(sparkSchema, avroSchema)

    new HoodieCdcBatchScan(
      requiredSchema,
      partitions,
      broadcastReader,
      broadcastConf,
      metaClient.getBasePath.toString,
      originTableSchema,
      options)
  }

  private def buildPartitions(
      fileSlicesPerPartition: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[org.apache.hudi.common.model.FileSlice])],
      fullPartSchema: StructType,
      requiredPartitionSchema: StructType): Array[InputPartition] = {

    fileSlicesPerPartition.flatMap { case (partitionOpt, fileSlices) =>
      fileSlices.filter(fs => fs.getBaseFile.isPresent || fs.hasLogFiles).map { fs =>
        val baseFilePath = if (fs.getBaseFile.isPresent) fs.getBaseFile.get().getPath else ""
        val baseFileLength = if (fs.getBaseFile.isPresent) fs.getBaseFile.get().getFileSize else 0L
        val allPartValues = partitionOpt.map(_.getValues).getOrElse(Array.empty[AnyRef])

        val partValues = if (requiredPartitionSchema.isEmpty) {
          Array.empty[AnyRef]
        } else {
          requiredPartitionSchema.fieldNames.map { name =>
            val idx = fullPartSchema.fieldIndex(name)
            allPartValues(idx)
          }
        }

        val logFiles = fs.getLogFiles.sorted(HoodieLogFile.getLogFileComparator)
          .iterator().asScala.toList
        val relPartPath = partitionOpt.map(_.getPath).getOrElse("")

        HoodieInputPartition(0, baseFilePath, baseFileLength, partValues, logFiles, relPartPath)
      }
    }.zipWithIndex.map { case (p, i) => p.copy(index = i) }.toArray[InputPartition]
  }

  private def buildMorContext(requiredDataSchema: StructType): Option[MorContext] = {
    if (includeLogFiles) {
      val tableName = metaClient.getTableConfig.getTableName
      val schemaResolver = new TableSchemaResolver(metaClient)
      val tableDataSchema = schemaResolver.getTableSchema
      val requiredMergeSchema = convertToHoodieSchema(requiredDataSchema, tableName)

      val latestCommitTimestamp = metaClient.getCommitsAndCompactionTimeline
        .filterCompletedInstants.lastInstant()
        .map[String](_.requestedTime)
        .orElse(null)

      val mergeType = options.getOrElse(
        HoodieReaderConfig.MERGE_TYPE.key(),
        HoodieReaderConfig.MERGE_TYPE.defaultValue())

      Some(MorContext(
        latestCommitTimestamp,
        tableDataSchema,
        requiredMergeSchema,
        requiredDataSchema,
        metaClient.getBasePath.toString,
        mergeType,
        options))
    } else {
      None
    }
  }
}
