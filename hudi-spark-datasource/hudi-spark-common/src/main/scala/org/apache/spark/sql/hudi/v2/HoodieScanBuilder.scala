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

import org.apache.hudi.{DataSourceReadOptions, HoodieBaseRelation, HoodieDataSourceHelper, HoodieFileIndex, SparkAdapterSupport}
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.config.HoodieBootstrapConfig
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.utils.SerDeHelper
import org.apache.hudi.io.storage.HoodieSparkParquetReader.ENABLE_LOGICAL_TIMESTAMP_REPAIR
import org.apache.hudi.keygen.{TimestampBasedAvroKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.hudi.util.SparkConfigUtils

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.HoodieSchemaRepair
import org.apache.spark.sql.{HoodieCatalystExpressionUtils, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownFilters, SupportsPushDownLimit, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * Scan builder for DSv2 snapshot reads of base files (COW snapshot and MOR
 * read_optimized).
 *
 * Supports column pruning and filter pushdown (partition pruning, metadata-table data
 * skipping, and Parquet row-group pruning). All pushed filters stay advisory:
 * [[pushFilters]] returns every filter back to Spark for post-scan re-application, since
 * file-slice pruning is file-granular and the Parquet filters only prune row groups.
 *
 * Limits are pushed as per-partition limits ([[isPartiallyPushed]] stays true so Spark
 * keeps the global limit) and only when no filters were pushed: every pushed filter is
 * re-applied post-scan, and capping rows before a residual filter would drop matches.
 *
 * Group-by-free COUNT/MIN/MAX aggregations are answered entirely from column-stats
 * metadata when [[HoodieAggregatePushDown]] deems the stats complete and exact — again
 * only when no filters were pushed — in which case [[build]] returns a [[HoodieLocalScan]]
 * holding the pre-computed result row.
 */
class HoodieScanBuilder(spark: SparkSession,
                        metaClient: HoodieTableMetaClient,
                        tableSchema: StructType,
                        options: Map[String, String]) extends ScanBuilder
  with SupportsPushDownRequiredColumns
  with SupportsPushDownFilters
  with SupportsPushDownLimit
  with SupportsPushDownAggregates
  with SparkAdapterSupport {

  private val log = LoggerFactory.getLogger(getClass)

  private var requiredSchema: StructType = tableSchema

  // Catalyst forms of the pushed filters, classified by pushFilters: partition filters drive
  // partition pruning, data filters drive metadata-table data skipping (column stats, record
  // index); both feed HoodieFileIndex.filterFileSlices in build().
  private var partitionFilterExpressions: Seq[Expression] = Seq.empty
  private var dataFilterExpressions: Seq[Expression] = Seq.empty
  // Source filters safe to hand the Parquet reader for row-group pruning (no partition-column
  // references) and the full set reported back through pushedFilters().
  private var pushedParquetFilters: Array[Filter] = Array.empty
  private var pushedFilterArray: Array[Filter] = Array.empty

  // Mirror DSv1 (HoodieBaseHadoopFsRelationFactory.specifiedQueryTimestamp): a time-travel
  // read resolves schemas as of this instant; HoodieFileIndex picks the same option up from
  // the options map and scopes file slices to it.
  private lazy val specifiedQueryInstant: Option[String] =
    options.get(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key)
      .map(HoodieSqlCommonUtils.formatQueryInstant)

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  /**
   * Classifies the pushed filters:
   *
   *  - filters convertible to Catalyst expressions referencing only partition columns become
   *    partition filters (partition pruning), mirroring DSv1's
   *    [[HoodieBaseRelation]] isPartitionPredicate split;
   *  - all other convertible filters (including mixed partition/data references) become data
   *    filters for metadata-table data skipping;
   *  - filters that cannot be converted are post-scan only — Spark re-applies them.
   *
   * For timestamp key generators the file index types partition columns as STRING because the
   * partition path holds keygen-formatted dates rather than the source values; a filter on a
   * non-string partition column can then not be evaluated against path-parsed values, so it is
   * demoted to a data filter instead of pruning (the SQL DSv1 path likewise cannot prune those).
   *
   * Filters referencing a partition column are never forwarded to Parquet: the column may be
   * absent from base files (drop_partition_columns, path extraction), and for timestamp key
   * generators the stored values differ from the path-formatted ones a converted filter targets.
   *
   * Every filter is returned for Spark to re-apply: file-slice pruning is file-granular and the
   * Parquet filters only prune row groups, so neither guarantees row-level filtering.
   */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val resolver = spark.sessionState.analyzer.resolver
    val partitionColumns = metaClient.getTableConfig.getPartitionFields.orElse(Array.empty[String])
    def isPartitionColumn(name: String): Boolean = partitionColumns.exists(resolver(name, _))

    val convertedFilters = filters.flatMap { filter =>
      val exprOpt = try {
        HoodieCatalystExpressionUtils.convertToCatalystExpression(filter, tableSchema)
      } catch {
        // convertToCatalystExpression asserts that referenced columns exist in the table
        // schema; degrade to post-scan-only instead of failing the scan, as DSv1 only warns.
        case _: AssertionError => None
        case NonFatal(_) => None
      }
      if (exprOpt.isEmpty) {
        log.warn(s"Failed to convert filter into Catalyst expression, it will not be pushed down: $filter")
      }
      exprOpt.map(expr => filter -> expr)
    }

    val (partitionRefOnly, dataRef) = convertedFilters.partition { case (_, expr) =>
      expr.references.nonEmpty && expr.references.forall { r =>
        isPartitionColumn(r.name) && (!isTimestampKeyGenerator || r.dataType == StringType)
      }
    }
    partitionFilterExpressions = partitionRefOnly.map(_._2).toSeq
    dataFilterExpressions = dataRef.map(_._2).toSeq
    pushedParquetFilters = filters.filter(_.references.forall(ref => !isPartitionColumn(ref)))
    pushedFilterArray = (convertedFilters.map(_._1) ++ pushedParquetFilters).distinct

    filters
  }

  override def pushedFilters(): Array[Filter] = pushedFilterArray

  private var pushedLimit: Option[Int] = None
  // Memoized aggregate resolution keyed by the Aggregation instance: Spark consults
  // supportCompletePushDown and then pushAggregation with the same instance, and the
  // resolution reads the metadata table, so it must not run twice.
  private var resolvedAggregation: Option[(Aggregation, Option[(StructType, InternalRow, Seq[String])])] = None
  private var pushedAggregation: Option[(StructType, InternalRow, Seq[String])] = None

  /**
   * Limits are applied per input partition (each reader stops after `limit` rows) and
   * reported as partially pushed so Spark re-applies the global limit. Pushing is refused
   * when any filter was pushed: all pushed filters are re-applied post-scan, and rows
   * counted toward the cap could then be filtered away, losing later matches. (Spark
   * already never pushes a limit past a residual Filter node; this guard keeps the
   * invariant local.) An aggregate-answering scan returns its final rows, so a limit on
   * top of it stays with Spark too.
   */
  override def pushLimit(limit: Int): Boolean = {
    if (pushedFilterArray.isEmpty && pushedAggregation.isEmpty) {
      pushedLimit = Some(limit)
      true
    } else {
      false
    }
  }

  override def isPartiallyPushed(): Boolean = true

  /**
   * Complete-only aggregate pushdown: both entry points resolve the aggregation against
   * column-stats metadata via [[HoodieAggregatePushDown]] and answer consistently, so
   * Spark either removes the aggregate entirely (complete pushdown over the
   * [[HoodieLocalScan]] built later) or keeps it untouched — this scan never produces
   * partial-aggregation rows. Aggregates are only answerable when no filters were pushed:
   * per-file stats cannot be combined under a filter (Spark also never pushes aggregates
   * when a residual filter remains).
   */
  override def supportCompletePushDown(aggregation: Aggregation): Boolean =
    resolveAggregation(aggregation).isDefined

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    pushedAggregation = resolveAggregation(aggregation)
    pushedAggregation.isDefined
  }

  private def resolveAggregation(aggregation: Aggregation): Option[(StructType, InternalRow, Seq[String])] = {
    resolvedAggregation match {
      case Some((prior, result)) if prior eq aggregation => result
      case _ =>
        val result = if (pushedFilterArray.nonEmpty) {
          None
        } else {
          HoodieAggregatePushDown.tryPushDown(spark, metaClient, tableSchema, options, aggregation)
        }
        resolvedAggregation = Some((aggregation, result))
        result
    }
  }

  private lazy val isTimestampKeyGenerator: Boolean = {
    val keyGenerator = metaClient.getTableConfig.getKeyGeneratorClassName
    keyGenerator != null &&
      (keyGenerator == classOf[TimestampBasedKeyGenerator].getCanonicalName ||
        keyGenerator == classOf[TimestampBasedAvroKeyGenerator].getCanonicalName)
  }

  override def build(): Scan = {
    pushedAggregation match {
      // A fully answered aggregation never touches data files: the result row was computed
      // from column-stats metadata during pushAggregation.
      case Some((aggSchema, aggRow, aggDescriptions)) =>
        new HoodieLocalScan(aggSchema, Array(aggRow), aggDescriptions)
      case None =>
        buildBatchScan()
    }
  }

  private def buildBatchScan(): Scan = {
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
    val internalSchemaOpt = fetchInternalSchema()
    embedInternalSchema(hadoopConf, internalSchemaOpt)
    val tableAvroSchema = fetchTableAvroSchema()
    if (tableAvroSchema.isPresent) {
      // Mirror DSv1 (HoodieFileGroupReaderBasedFileFormat): skip the per-file footer repair
      // when no timestamp-millis field exists; the flag must be set before the reader is
      // built because SparkXXParquetReader.build reads it from the hadoop conf.
      hadoopConf.set(ENABLE_LOGICAL_TIMESTAMP_REPAIR,
        HoodieSchemaRepair.hasTimestampMillisField(tableAvroSchema.get).toString)
    }
    // Row mode: HoodiePartitionReader is a PartitionReader[InternalRow], so the columnar
    // reader must not hand back ColumnarBatches.
    val readerOptions = options + (FileFormat.OPTION_RETURNING_BATCH -> "false")
    val reader = sparkAdapter.createParquetFileReader(false, spark.sessionState.conf, readerOptions, hadoopConf)
    val broadcastReader = spark.sparkContext.broadcast(reader)
    val broadcastConf = spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    // Partition filters prune partitions and data filters drive metadata-table data skipping
    // (column stats, record index). Timestamp keygen partition filters must be converted to
    // the keygen output format first: this file index is built with shouldEmbedFileSlices =
    // false, so filterFileSlices does not convert them itself, unlike the SQL DSv1 path.
    val convertedPartitionFilters =
      HoodieFileIndex.convertFilterForTimestampKeyGenerator(metaClient, partitionFilterExpressions)
    val fileSlicesPerPartition = fileIndex.filterFileSlices(dataFilterExpressions, convertedPartitionFilters)

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
      requiredPartitionSchema,
      internalSchemaOpt,
      tableAvroSchema,
      pushedFilterArray,
      pushedParquetFilters,
      pushedLimit)
  }

  /**
   * Mirror DSv1 (HoodieBaseHadoopFsRelationFactory): fetch the internal schema from commit
   * metadata only when schema-on-read evolution is enabled (option or session conf), as of
   * the time-travel instant when one is supplied so evolved column IDs/types match the
   * snapshot being read. Resolution failures degrade to a plain read, as in DSv1.
   */
  private def fetchInternalSchema(): HOption[InternalSchema] = {
    if (!HoodieBaseRelation.isSchemaEvolutionEnabledOnRead(options, spark)) {
      HOption.empty[InternalSchema]()
    } else {
      try {
        val schemaResolver = new TableSchemaResolver(metaClient)
        specifiedQueryInstant match {
          case Some(ts) => schemaResolver.getTableInternalSchemaFromCommitMetadata(ts)
          case None => schemaResolver.getTableInternalSchemaFromCommitMetadata
        }
      } catch {
        case e: Exception =>
          log.warn("Failed to fetch internal schema from commit metadata; reading without schema-on-read evolution", e)
          HOption.empty[InternalSchema]()
      }
    }
  }

  /**
   * Embeds the schema-on-read context into the hadoop conf the executors read with:
   * ParquetSchemaEvolutionUtils resolves each base file's writer schema by commit time via
   * InternalSchemaCache, which requires the table path and the completed-instant file names
   * (and legacy readers take the query schema itself) from the conf.
   */
  private def embedInternalSchema(conf: Configuration, internalSchemaOpt: HOption[InternalSchema]): Unit = {
    if (internalSchemaOpt.isPresent) {
      val fileNameGenerator = metaClient.getInstantFileNameGenerator
      val validCommits = metaClient.getCommitsAndCompactionTimeline.filterCompletedInstants
        .getInstants.iterator.asScala.map(fileNameGenerator.getFileName).mkString(",")
      conf.set(SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA, SerDeHelper.toJson(internalSchemaOpt.get))
      conf.set(SparkInternalSchemaConverter.HOODIE_TABLE_PATH, metaClient.getBasePath.toString)
      conf.set(SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST, validCommits)
    }
  }

  /**
   * Mirror DSv1 (HoodieFileGroupReaderBasedFileFormat.tableSchemaAsMessageType): resolve the
   * table Avro schema — as of the time-travel instant when one is supplied — for the Parquet
   * footer logical-type repair in the reader. Failures degrade to no repair rather than
   * failing the scan.
   */
  private def fetchTableAvroSchema(): HOption[HoodieSchema] = {
    try {
      val schemaResolver = new TableSchemaResolver(metaClient)
      val schema = specifiedQueryInstant match {
        case Some(ts) => schemaResolver.getTableSchema(ts)
        case None => schemaResolver.getTableSchema
      }
      HOption.ofNullable(schema)
    } catch {
      case e: Exception =>
        log.warn("Failed to fetch table schema for Parquet logical-type repair", e)
        HOption.empty[HoodieSchema]()
    }
  }
}
