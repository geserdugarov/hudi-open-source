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
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieReaderConfig}
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.model.{HoodieLogFile, HoodieTableType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.common.table.cdc.HoodieCDCExtractor
import org.apache.hudi.common.table.log.InstantRange
import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer
import org.apache.hudi.common.table.timeline.TimelineUtils.getCommitMetadata
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.listAffectedFilesForCommits
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, MetadataPartitionType}
import org.apache.hudi.metadata.HoodieTableMetadataUtil.getWritePartitionPaths
import org.apache.hudi.stats.HoodieColumnRangeMetadata

import org.apache.spark.sql.HoodieCatalystExpressionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation, Count, CountStar, Max, Min}
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
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
  with PartialLimitPushDown
  with SupportsPushDownAggregates
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

  private var pushedLimit: Option[Int] = None
  private var pushedAggregation: Option[Aggregation] = None
  private var aggregateResult: Option[Array[InternalRow]] = None

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

  override def pushLimit(limit: Int): Boolean = {
    pushedLimit = Some(limit)
    true
  }

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    if (isCdcQuery || isIncrementalQuery || isReadOptimized) {
      false
    } else if (aggregation.groupByExpressions().nonEmpty) {
      false
    } else if (!MetadataPartitionType.COLUMN_STATS.isMetadataPartitionAvailable(metaClient)) {
      false
    } else {
      val funcs = aggregation.aggregateExpressions()
      val allSupported = funcs.forall {
        case _: CountStar => true
        case c: Count => !c.isDistinct
        case _: Min => true
        case _: Max => true
        case _ => false
      }
      if (!allSupported) {
        false
      } else {
        tryComputeAggregates(aggregation) match {
          case Some(rows) =>
            pushedAggregation = Some(aggregation)
            aggregateResult = Some(rows)
            true
          case None => false
        }
      }
    }
  }

  override def supportCompletePushDown(aggregation: Aggregation): Boolean = {
    pushedAggregation.contains(aggregation) && dataFilterExprs.isEmpty
  }

  override def build(): Scan = {
    aggregateResult match {
      case Some(rows) =>
        val outputSchema = buildAggregateOutputSchema(pushedAggregation.get)
        new HoodieLocalScan(outputSchema, rows)
      case None =>
        if (isCdcQuery) buildCdcScan()
        else if (isIncrementalQuery) buildIncrementalScan()
        else buildSnapshotScan()
    }
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
      morCtx,
      pushedLimit = pushedLimit)
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
        _pushedFilters,
        pushedLimit = pushedLimit)
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
        Some(includedCommitTimes),
        pushedLimit)
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

  private def tryComputeAggregates(aggregation: Aggregation): Option[Array[InternalRow]] = {
    try {
      val fileSlicesPerPartition = fileIndex.filterFileSlices(dataFilterExprs, partitionFilterExprs)

      // MoR with log files: column stats don't reflect updates/deletes in logs
      val hasLogFiles = fileSlicesPerPartition.exists { case (_, slices) =>
        slices.exists(_.hasLogFiles)
      }
      if (hasLogFiles || dataFilterExprs.nonEmpty) {
        None
      } else {
        // Collect base files as (partitionPath, fileName) pairs
        val baseFiles = fileSlicesPerPartition.flatMap { case (partOpt, slices) =>
          slices.filter(_.getBaseFile.isPresent).map { fs =>
            val partPath = partOpt.map(_.getPath).getOrElse("")
            val fileName = fs.getBaseFile.get().getFileName
            (partPath, fileName)
          }
        }

        if (baseFiles.isEmpty) {
          Some(Array(buildEmptyAggregateRow(aggregation)))
        } else {
          computeAggregatesFromStats(aggregation, baseFiles)
        }
      }
    } catch {
      case _: Exception => None
    }
  }

  private def computeAggregatesFromStats(
      aggregation: Aggregation,
      baseFiles: Seq[(String, String)]): Option[Array[InternalRow]] = {
    val aggFuncs = aggregation.aggregateExpressions()

    // Determine which columns need stats; None if unsupported function found
    val referencedColumnsOpt = aggFuncs.foldLeft(Option(Seq.empty[String])) { (accOpt, func) =>
      accOpt.flatMap { acc =>
        func match {
          case _: CountStar => Some(acc)
          case c: Count => extractColumnName(c.column()).map(name => acc :+ name)
          case m: Min => extractColumnName(m.column()).map(name => acc :+ name)
          case m: Max => extractColumnName(m.column()).map(name => acc :+ name)
          case _ => None
        }
      }
    }

    referencedColumnsOpt.flatMap { referencedColumns =>
      val distinctColumns = referencedColumns.distinct

      // For CountStar, use the first table column to get total row count
      val countStarColumnOpt: Option[Option[String]] = if (aggFuncs.exists(_.isInstanceOf[CountStar])) {
        if (tableSchema.nonEmpty) Some(Some(tableSchema.fields.head.name)) else None
      } else {
        Some(None)
      }

      countStarColumnOpt.flatMap { countStarColumn =>
        val allColumns = (distinctColumns ++ countStarColumn.toSeq).distinct
        if (allColumns.isEmpty) {
          None
        } else {
          queryAndComputeAggregates(aggFuncs, allColumns, countStarColumn, baseFiles)
        }
      }
    }
  }

  private def queryAndComputeAggregates(
      aggFuncs: Array[org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc],
      allColumns: Seq[String],
      countStarColumn: Option[String],
      baseFiles: Seq[(String, String)]): Option[Array[InternalRow]] = {
    val metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build()
    val engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf)
    val tableMetadata = new HoodieBackedTableMetadata(
      engineContext, metaClient.getStorage, metadataConfig, metaClient.getBasePath.toString)

    try {
      val filePairs = baseFiles.map { case (part, file) => Pair.of(part, file) }.asJava

      // Query stats for all needed columns; None if any column has incomplete stats
      val columnStatsOpt = allColumns.foldLeft(
        Option(Map.empty[String, Seq[HoodieColumnRangeMetadata[Comparable[_]]]])
      ) { (accOpt, col) =>
        accOpt.flatMap { acc =>
          val statsMap = tableMetadata.getColumnStats(filePairs, col)
          if (statsMap.size() != baseFiles.size) {
            None
          } else {
            val allFileStats = statsMap.values().asScala
              .map(cs => HoodieColumnRangeMetadata.fromColumnStats(cs)).toSeq
            Some(acc + (col -> allFileStats))
          }
        }
      }

      columnStatsOpt.flatMap { columnStats =>
        // Compute each aggregate value as Option; None means unsupported
        val valuesOpt: Array[Option[Any]] = aggFuncs.map {
          case _: CountStar =>
            val stats = columnStats(countStarColumn.get)
            Some(stats.map(s => s.getValueCount + s.getNullCount).sum: Any)

          case c: Count =>
            extractColumnName(c.column()).map { colName =>
              val stats = columnStats(colName)
              stats.map(_.getValueCount).sum: Any
            }

          case m: Min =>
            for {
              colName <- extractColumnName(m.column())
              colType <- tableSchema.fields.find(_.name == colName).map(_.dataType)
              result <- computeMinMax(columnStats(colName), colType, isMin = true)
            } yield result

          case m: Max =>
            for {
              colName <- extractColumnName(m.column())
              colType <- tableSchema.fields.find(_.name == colName).map(_.dataType)
              result <- computeMinMax(columnStats(colName), colType, isMin = false)
            } yield result

          case _ => None
        }

        if (valuesOpt.forall(_.isDefined)) {
          Some(Array(new GenericInternalRow(valuesOpt.map(_.get))))
        } else {
          None
        }
      }
    } finally {
      tableMetadata.close()
    }
  }

  private def computeMinMax(
      allFileStats: Seq[HoodieColumnRangeMetadata[Comparable[_]]],
      colType: DataType,
      isMin: Boolean): Option[Any] = {
    val rawValues = allFileStats.map(s => if (isMin) s.getMinValue else s.getMaxValue).filter(_ != null)
    if (rawValues.isEmpty) {
      Some(null)
    } else {
      val sparkValues = rawValues.flatMap(v => convertToSparkValue(v, colType))
      if (sparkValues.size != rawValues.size) {
        None
      } else {
        colType match {
          case IntegerType =>
            val vs = sparkValues.map(_.asInstanceOf[Int])
            Some(if (isMin) vs.min else vs.max)
          case LongType =>
            val vs = sparkValues.map(_.asInstanceOf[Long])
            Some(if (isMin) vs.min else vs.max)
          case FloatType =>
            val vs = sparkValues.map(_.asInstanceOf[Float])
            Some(if (isMin) vs.min else vs.max)
          case DoubleType =>
            val vs = sparkValues.map(_.asInstanceOf[Double])
            Some(if (isMin) vs.min else vs.max)
          case StringType =>
            val vs = sparkValues.map(_.asInstanceOf[UTF8String])
            Some(vs.reduce((a, b) => if ((isMin && a.compareTo(b) <= 0) || (!isMin && a.compareTo(b) >= 0)) a else b))
          case _ => None
        }
      }
    }
  }

  private def buildEmptyAggregateRow(aggregation: Aggregation): InternalRow = {
    val values = aggregation.aggregateExpressions().map {
      case _: CountStar => 0L: Any
      case _: Count => 0L: Any
      case _ => null: Any
    }
    new GenericInternalRow(values)
  }

  private def buildAggregateOutputSchema(aggregation: Aggregation): StructType = {
    val fields = aggregation.aggregateExpressions().zipWithIndex.map { case (func, i) =>
      func match {
        case _: CountStar =>
          StructField(s"count(*)", LongType, nullable = false)
        case c: Count =>
          val colName = extractColumnName(c.column()).getOrElse(s"col$i")
          StructField(s"count($colName)", LongType, nullable = true)
        case m: Min =>
          val colName = extractColumnName(m.column()).getOrElse(s"col$i")
          val colType = tableSchema.fields.find(_.name == colName).map(_.dataType).getOrElse(LongType)
          StructField(s"min($colName)", colType, nullable = true)
        case m: Max =>
          val colName = extractColumnName(m.column()).getOrElse(s"col$i")
          val colType = tableSchema.fields.find(_.name == colName).map(_.dataType).getOrElse(LongType)
          StructField(s"max($colName)", colType, nullable = true)
        case _ =>
          StructField(s"agg$i", LongType, nullable = true)
      }
    }
    StructType(fields)
  }

  private def extractColumnName(
      expr: org.apache.spark.sql.connector.expressions.Expression): Option[String] = {
    expr match {
      case ref: NamedReference =>
        val names = ref.fieldNames()
        if (names.length == 1) Some(names.head) else None
      case _ => None
    }
  }

  private def convertToSparkValue(value: Any, dataType: DataType): Option[Any] = {
    if (value == null) {
      Some(null)
    } else try {
      dataType match {
        case BooleanType => Some(value.asInstanceOf[Boolean])
        case ByteType => Some(value.asInstanceOf[Number].byteValue())
        case ShortType => Some(value.asInstanceOf[Number].shortValue())
        case IntegerType => Some(value.asInstanceOf[Number].intValue())
        case LongType => Some(value.asInstanceOf[Number].longValue())
        case FloatType => Some(value.asInstanceOf[Number].floatValue())
        case DoubleType => Some(value.asInstanceOf[Number].doubleValue())
        case StringType => Some(UTF8String.fromString(value.toString))
        case _ => None
      }
    } catch {
      case _: Exception => None
    }
  }
}
