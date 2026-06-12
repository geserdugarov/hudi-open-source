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

import org.apache.hudi.{ColumnStatsIndexSupport, DataSourceReadOptions, HoodieFileIndex, SparkAdapterSupport}
import org.apache.hudi.avro.model.HoodieMetadataColumnStats
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.metadata.HoodieTableMetadataUtil
import org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS
import org.apache.hudi.stats.ValueMetadata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, NamedReference}
import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation, Count, CountStar, Max, Min}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * Answers group-by-free COUNT(*) / COUNT(col) / MIN(col) / MAX(col) aggregations from the
 * metadata-table column-stats index at planning time, without reading any data file.
 *
 * An aggregation is answered only when the stats constitute complete, exact information
 * for every planned base file:
 *
 *  - only called when no filters were pushed — a filtered scan cannot be summarized from
 *    per-file stats (Spark additionally never pushes aggregates past a residual filter);
 *  - every aggregate references a top-level primitive column of the table schema (or is
 *    COUNT(*)); DISTINCT counts are not answerable from stats;
 *  - MIN/MAX on float/double are rejected: column stats follow Parquet semantics and drop
 *    NaN when collecting bounds, while Spark orders NaN above any other value, so a file
 *    containing NaN would make stats-derived bounds wrong;
 *  - every planned base file must have a live stats record for every referenced column
 *    (no record — e.g. files predating a column added by schema evolution — means the
 *    aggregation is not answerable and the query falls back to a regular scan);
 *  - a stats record with null bounds counts as complete only when the column is all-null
 *    in that file (nullCount == valueCount); otherwise it is a stub and forces fallback.
 *
 * COUNT(*) sums the per-file `valueCount` of one completely indexed top-level primitive
 * column (each such record counts one value per row, nulls included): the first referenced
 * column when there is one, else the record-key meta column — present in every base file
 * since the first commit, hence immune to schema evolution — else the first indexed
 * primitive column in schema order. COUNT(col) sums `valueCount - nullCount`; MIN/MAX fold
 * the per-file bounds in Spark's internal representation (binary comparison for strings).
 */
object HoodieAggregatePushDown extends SparkAdapterSupport {

  private val log = LoggerFactory.getLogger(getClass)

  private sealed trait StatsAggregate
  private case object CountStarAggregate extends StatsAggregate
  private case class CountColumnAggregate(field: StructField) extends StatsAggregate
  private case class MinAggregate(field: StructField) extends StatsAggregate
  private case class MaxAggregate(field: StructField) extends StatsAggregate

  /**
   * Attempts to answer `aggregation` from column-stats metadata. Returns the scan output
   * schema (one field per aggregate, matching what Spark expects for complete pushdown:
   * LongType for counts, the column type for MIN/MAX), the single result row in Spark's
   * internal representation, and display forms of the aggregates; or None when the
   * aggregation cannot be answered exactly, in which case the caller must fall back to a
   * regular scan.
   */
  def tryPushDown(spark: SparkSession,
                  metaClient: HoodieTableMetaClient,
                  tableSchema: StructType,
                  options: Map[String, String],
                  aggregation: Aggregation): Option[(StructType, InternalRow, Seq[String])] = {
    val specsOpt = if (aggregation.groupByExpressions().nonEmpty) {
      None
    } else {
      translateAggregates(spark, tableSchema, aggregation)
    }
    specsOpt.flatMap { specs =>
      val valuesOpt = try {
        computeFromColumnStats(spark, metaClient, tableSchema, options, specs)
      } catch {
        // Metadata-table availability must never fail the query: degrade to a full scan.
        case NonFatal(e) =>
          log.warn("Failed to answer aggregate pushdown from column stats; falling back to scan", e)
          None
      }
      valuesOpt.map { values =>
        val outputSchema = StructType(specs.zipWithIndex.map {
          case (CountStarAggregate, i) => StructField(s"agg_func_$i", LongType, nullable = false)
          case (CountColumnAggregate(_), i) => StructField(s"agg_func_$i", LongType, nullable = false)
          case (MinAggregate(field), i) => StructField(s"agg_func_$i", field.dataType, nullable = true)
          case (MaxAggregate(field), i) => StructField(s"agg_func_$i", field.dataType, nullable = true)
        })
        val descriptions = aggregation.aggregateExpressions().map(_.toString).toSeq
        (outputSchema, new GenericInternalRow(values.toArray): InternalRow, descriptions)
      }
    }
  }

  /** Maps each V2 aggregate function to a stats-answerable form, or None if any is not. */
  private def translateAggregates(spark: SparkSession,
                                  tableSchema: StructType,
                                  aggregation: Aggregation): Option[Seq[StatsAggregate]] = {
    val resolver = spark.sessionState.analyzer.resolver
    def topLevelPrimitiveField(expr: V2Expression): Option[StructField] = expr match {
      case ref: NamedReference if ref.fieldNames().length == 1 =>
        tableSchema.fields.find(f => resolver(f.name, ref.fieldNames().head)).filter(f => isPrimitive(f.dataType))
      case _ => None
    }

    val translated: Array[Option[StatsAggregate]] = aggregation.aggregateExpressions().map {
      case _: CountStar => Some(CountStarAggregate)
      case count: Count if !count.isDistinct => topLevelPrimitiveField(count.column()).map(CountColumnAggregate)
      case min: Min => topLevelPrimitiveField(min.column()).filter(f => supportsMinMax(f.dataType)).map(MinAggregate)
      case max: Max => topLevelPrimitiveField(max.column()).filter(f => supportsMinMax(f.dataType)).map(MaxAggregate)
      case _ => None
    }
    if (translated.nonEmpty && translated.forall(_.isDefined)) Some(translated.map(_.get).toSeq) else None
  }

  private def isPrimitive(dataType: DataType): Boolean = dataType match {
    case _: StructType | _: ArrayType | _: MapType => false
    case _ => true
  }

  /**
   * Types whose stats bounds are exact and totally ordered in Spark's internal form.
   * Float/double are excluded for NaN (see class doc); binary has no stats ordering
   * guarantee matching Spark's.
   */
  private def supportsMinMax(dataType: DataType): Boolean = dataType match {
    case FloatType | DoubleType => false
    case BooleanType | ByteType | ShortType | IntegerType | LongType | StringType | DateType | TimestampType => true
    case _: DecimalType => true
    case dt => sparkAdapter.isTimestampNTZType(dt)
  }

  /** Computes one internal value per spec, or None when the stats are not complete and exact. */
  private def computeFromColumnStats(spark: SparkSession,
                                     metaClient: HoodieTableMetaClient,
                                     tableSchema: StructType,
                                     options: Map[String, String],
                                     specs: Seq[StatsAggregate]): Option[Seq[Any]] = {
    // Enumerate exactly the base files an unfiltered batch scan would read: the latest file
    // slices, scoped to the time-travel instant when one is set (HoodieFileIndex picks it
    // up from the options), ignoring log-only file groups just like the scan does.
    val fileIndex = HoodieFileIndex(spark, metaClient, Some(tableSchema), options,
      includeLogFiles = false, shouldEmbedFileSlices = false)
    val baseFileNames = fileIndex.filterFileSlices(Seq.empty, Seq.empty)
      .flatMap(_._2).filter(_.getBaseFile.isPresent).map(_.getBaseFile.get().getFileName).toSeq

    if (baseFileNames.isEmpty) {
      // Empty snapshot: counts are 0 and MIN/MAX are NULL, no metadata needed.
      Some(specs.map {
        case CountStarAggregate | _: CountColumnAggregate => 0L: Any
        case _: MinAggregate | _: MaxAggregate => null
      })
    } else {
      computeFromCompleteStats(spark, metaClient, tableSchema, options, specs, baseFileNames)
    }
  }

  /** Computes the values over a non-empty file set; None when stats are missing or inexact. */
  private def computeFromCompleteStats(spark: SparkSession,
                                       metaClient: HoodieTableMetaClient,
                                       tableSchema: StructType,
                                       options: Map[String, String],
                                       specs: Seq[StatsAggregate],
                                       baseFileNames: Seq[String]): Option[Seq[Any]] = {
    // The table schema as of the queried instant, for index-column validation. Failures
    // degrade to a regular scan via tryPushDown's catch rather than failing the query.
    val schemaResolver = new TableSchemaResolver(metaClient)
    val hoodieSchema =
      options.get(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key).map(HoodieSqlCommonUtils.formatQueryInstant) match {
        case Some(ts) => schemaResolver.getTableSchema(ts)
        case None => schemaResolver.getTableSchema
      }

    // Mirror BaseHoodieTableFileIndex's metadata config so index availability is judged the
    // same way the file index judges it.
    val configProperties = HoodieFileIndex.getConfigProperties(spark, options, metaClient.getTableConfig)
    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(configProperties)
      .enable(configProperties.getBoolean(HoodieMetadataConfig.ENABLE.key, HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS)
        && HoodieTableMetadataUtil.isFilesPartitionAvailable(metaClient))
      .build()

    val columnStatsIndex = new ColumnStatsIndexSupport(spark, tableSchema, hoodieSchema, metadataConfig, metaClient)
    val indexDefinition = metaClient.getIndexForMetadataPartition(PARTITION_NAME_COLUMN_STATS)
    val referencedColumns = specs.collect {
      case CountColumnAggregate(field) => field.name
      case MinAggregate(field) => field.name
      case MaxAggregate(field) => field.name
    }.distinct

    val targetColumnsOpt: Option[(Seq[String], Option[String])] =
      if (!columnStatsIndex.isIndexAvailable || !indexDefinition.isPresent) {
        None
      } else {
        val validIndexedColumns = HoodieTableMetadataUtil
          .getValidIndexedColumns(indexDefinition.get, hoodieSchema, metaClient.getTableConfig).asScala.toSet
        val countStarColumn = if (specs.contains(CountStarAggregate)) {
          referencedColumns.headOption.orElse(pickCountStarColumn(tableSchema, validIndexedColumns))
        } else {
          None
        }
        if (referencedColumns.forall(validIndexedColumns.contains)
          && (countStarColumn.isDefined || !specs.contains(CountStarAggregate))) {
          Some(((referencedColumns ++ countStarColumn).distinct, countStarColumn))
        } else {
          None
        }
      }

    targetColumnsOpt.flatMap { case (targetColumns, countStarColumn) =>
      computeValues(spark, columnStatsIndex, metadataConfig, specs, baseFileNames, targetColumns, countStarColumn)
    }
  }

  /** Folds the loaded stats records into one internal value per spec. */
  private def computeValues(spark: SparkSession,
                            columnStatsIndex: ColumnStatsIndexSupport,
                            metadataConfig: HoodieMetadataConfig,
                            specs: Seq[StatsAggregate],
                            baseFileNames: Seq[String],
                            targetColumns: Seq[String],
                            countStarColumn: Option[String]): Option[Seq[Any]] = {
    val readInMemory = shouldReadStatsInMemory(metadataConfig, baseFileNames.size, targetColumns.size)
    val statsByFileAndColumn = columnStatsIndex
      .loadColumnStatsIndexRecords(targetColumns, shouldReadInMemory = readInMemory)
      .collectAsList().asScala
      .filterNot(_.getIsDeleted)
      .map(r => (String.valueOf(r.getFileName), String.valueOf(r.getColumnName)) -> r)
      .toMap

    /** The column's stats records covering every planned base file, or None if any is missing. */
    def completeStatsFor(column: String): Option[Seq[HoodieMetadataColumnStats]] = {
      val perFile = baseFileNames.map(file => statsByFileAndColumn.get((file, column)))
      if (perFile.forall(_.isDefined)) Some(perFile.map(_.get)) else None
    }

    lazy val rowCount: Option[Long] = countStarColumn.flatMap { column =>
      completeStatsFor(column).flatMap { records =>
        records.foldLeft(Option(0L)) { (acc, record) =>
          for { total <- acc; valueCount <- Option(record.getValueCount) } yield total + valueCount
        }
      }
    }

    def nonNullCount(field: StructField): Option[Long] =
      completeStatsFor(field.name).flatMap { records =>
        records.foldLeft(Option(0L)) { (acc, record) =>
          for {
            total <- acc
            valueCount <- Option(record.getValueCount)
            nullCount <- Option(record.getNullCount)
          } yield total + (valueCount - nullCount)
        }
      }

    val useJava8api = spark.sessionState.conf.datetimeJava8ApiEnabled
    // Some(value-or-null) when answerable (null = no non-null values at all), None when not.
    def bound(field: StructField, isMin: Boolean): Option[Any] =
      completeStatsFor(field.name).flatMap { records =>
        val converter = CatalystTypeConverters.createToCatalystConverter(field.dataType)
        val folded = records.foldLeft(Option(Option.empty[Any])) { (accOpt, record) =>
          accOpt.flatMap { acc =>
            if (record.getMinValue == null && record.getMaxValue == null) {
              // All-null column in this file (complete information) versus a stubbed
              // tombstone record (no information — forces fallback).
              (Option(record.getValueCount), Option(record.getNullCount)) match {
                case (Some(valueCount), Some(nullCount)) if valueCount.longValue() == nullCount.longValue() => Some(acc)
                case _ => None
              }
            } else {
              try {
                val wrapper = if (isMin) record.getMinValue else record.getMaxValue
                val valueMetadata = ValueMetadata.getValueMetadata(record.getValueType)
                val external = ColumnStatsIndexSupport.extractColStatsValue(wrapper, field.dataType, valueMetadata, useJava8api)
                val internal = converter(external)
                if (internal == null) {
                  None
                } else {
                  Some(Some(acc.fold(internal) { current =>
                    val cmp = internal.asInstanceOf[Comparable[Any]].compareTo(current)
                    if ((isMin && cmp < 0) || (!isMin && cmp > 0)) internal else current
                  }))
                }
              } catch {
                case NonFatal(e) =>
                  log.warn(s"Failed to decode column stats bound for column '${field.name}'; falling back to scan", e)
                  None
              }
            }
          }
        }
        folded.map(_.orNull)
      }

    val values = specs.map {
      case CountStarAggregate => rowCount.map(c => c: Any)
      case CountColumnAggregate(field) => nonNullCount(field).map(c => c: Any)
      case MinAggregate(field) => bound(field, isMin = true)
      case MaxAggregate(field) => bound(field, isMin = false)
    }
    if (values.forall(_.isDefined)) Some(values.map(_.get)) else None
  }

  /**
   * Picks the column whose per-file `valueCount` supplies COUNT(*). Prefer the record-key
   * meta column (present in every base file since the first commit, hence immune to schema
   * evolution), then fall back to the first indexed top-level primitive column in schema
   * order. Nested/complex columns are excluded because their stats count leaf values, not
   * rows.
   */
  private def pickCountStarColumn(tableSchema: StructType, validIndexedColumns: Set[String]): Option[String] = {
    val candidates = tableSchema.fields
      .filter(f => isPrimitive(f.dataType) && validIndexedColumns.contains(f.name))
      .map(_.name)
    candidates.find(_ == HoodieRecord.RECORD_KEY_METADATA_FIELD).orElse(candidates.headOption)
  }

  private def shouldReadStatsInMemory(metadataConfig: HoodieMetadataConfig, fileCount: Int, columnCount: Int): Boolean = {
    // Same heuristic as SparkBaseIndexSupport.shouldReadInMemory: small projections are
    // cheaper to read on the driver than to round-trip through a Spark job.
    Option(metadataConfig.getColumnStatsIndexProcessingModeOverride) match {
      case Some(mode) => mode == HoodieMetadataConfig.COLUMN_STATS_INDEX_PROCESSING_MODE_IN_MEMORY
      case None => fileCount.toLong * columnCount < metadataConfig.getColumnStatsIndexInMemoryProjectionThreshold
    }
  }
}
