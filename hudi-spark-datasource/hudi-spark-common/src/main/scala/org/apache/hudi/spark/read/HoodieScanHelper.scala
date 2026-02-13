/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.spark.read

import org.apache.hudi.HoodieBaseRelation
import org.apache.hudi.HoodieFileIndex
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.internal.schema.InternalSchema

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.NoopCache
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{BooleanType, StructType}

import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Scala helper for DSv2 read classes to interact with {@link HoodieFileIndex}.
 *
 * <p>Bridges the Scala-heavy {@code HoodieFileIndex} API into Java-friendly
 * return types so that the Java DSv2 classes ({@link HoodieScan},
 * {@link HoodiePartitionReader}, etc.) can remain pure Java.
 */
object HoodieScanHelper {

  /**
   * Plans input partitions for a DSv2 read by creating a {@link HoodieFileIndex}
   * and collecting file slices after applying partition and data filters.
   *
   * @param spark            active SparkSession (driver side)
   * @param tablePath        base path of the Hudi table
   * @param tableSchema      full table schema
   * @param options          merged table properties and read options
   * @param partitionFilters Spark filters on partition columns
   * @param dataFilters      Spark filters on data columns
   * @return a list of {@link HoodieInputPartition} ready to be shipped to executors
   */
  def planInputPartitions(spark: SparkSession,
                          tablePath: String,
                          tableSchema: StructType,
                          options: JMap[String, String],
                          partitionFilters: Array[sources.Filter],
                          dataFilters: Array[sources.Filter]): JList[HoodieInputPartition] = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val storageConf = HadoopFSUtils.getStorageConf(hadoopConf)
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(storageConf)
      .setBasePath(tablePath)
      .build()

    val scalaOptions = options.asScala.toMap
    val fileIndex = HoodieFileIndex(spark, metaClient, Some(tableSchema),
      scalaOptions, NoopCache, includeLogFiles = true, shouldEmbedFileSlices = true)

    // Convert Spark source Filters to catalyst Expressions for HoodieFileIndex
    val attributes = tableSchema.map(f =>
      AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
    val partitionExprs = filtersToExpressions(partitionFilters, attributes)
    val dataExprs = filtersToExpressions(dataFilters, attributes)

    val allFileSlices = fileIndex.filterFileSlices(
      dataFilters = dataExprs,
      partitionFilters = partitionExprs)

    val lastInstantOpt = metaClient.getCommitsAndCompactionTimeline
      .filterCompletedInstants().lastInstant()
    val latestCommitTime = if (lastInstantOpt.isPresent) {
      lastInstantOpt.get.requestedTime
    } else {
      ""
    }

    val partitions = new JArrayList[HoodieInputPartition]()
    var index = 0
    for ((_, fileSlices) <- allFileSlices) {
      for (fs <- fileSlices) {
        partitions.add(new HoodieInputPartition(index, fs, fs.getPartitionPath,
          tablePath, latestCommitTime))
        index += 1
      }
    }

    partitions
  }

  /**
   * Converts an array of Spark source {@code Filter}s to catalyst
   * {@code Expression}s, resolving column references against the given
   * attributes. Filters that cannot be converted are silently dropped.
   */
  private def filtersToExpressions(
      filters: Array[sources.Filter],
      attributes: Seq[Attribute]): Seq[Expression] = {
    val attrMap: Map[String, Attribute] = attributes.map(a => a.name -> a).toMap
    filters.flatMap(f => filterToExpression(f, attrMap)).toSeq
  }

  /**
   * Converts a single Spark source {@code Filter} to a catalyst
   * {@code Expression}. Adapted from
   * {@code FileFormatUtilsForFileGroupReader.filterToExpression}.
   */
  private def filterToExpression(
      filter: sources.Filter,
      attrMap: Map[String, Attribute]): Option[Expression] = {

    def toRef(name: String): Option[NamedExpression] = attrMap.get(name)

    def toLiteral(value: Any): Option[Literal] = Try(Literal(value)).toOption

    def zip[A, B](a: Option[A], b: Option[B]): Option[(A, B)] =
      a.zip(b).headOption

    def zipAttrAndValue(name: String, value: Any): Option[(NamedExpression, Literal)] =
      zip(toRef(name), toLiteral(value))

    def translate(f: sources.Filter): Option[Expression] = f match {
      case sources.And(left, right) =>
        zip(translate(left), translate(right)).map(And.tupled)
      case sources.Or(left, right) =>
        zip(translate(left), translate(right)).map(Or.tupled)
      case sources.Not(child) =>
        translate(child).map(Not)
      case sources.EqualTo(attr, value) =>
        zipAttrAndValue(attr, value).map(EqualTo.tupled)
      case sources.EqualNullSafe(attr, value) =>
        zipAttrAndValue(attr, value).map(EqualNullSafe.tupled)
      case sources.IsNull(attr) =>
        toRef(attr).map(IsNull)
      case sources.IsNotNull(attr) =>
        toRef(attr).map(IsNotNull)
      case sources.In(attr, values) =>
        val literals = values.toSeq.flatMap(toLiteral)
        if (literals.length == values.length) {
          toRef(attr).map(In(_, literals))
        } else {
          None
        }
      case sources.GreaterThan(attr, value) =>
        zipAttrAndValue(attr, value).map(GreaterThan.tupled)
      case sources.GreaterThanOrEqual(attr, value) =>
        zipAttrAndValue(attr, value).map(GreaterThanOrEqual.tupled)
      case sources.LessThan(attr, value) =>
        zipAttrAndValue(attr, value).map(LessThan.tupled)
      case sources.LessThanOrEqual(attr, value) =>
        zipAttrAndValue(attr, value).map(LessThanOrEqual.tupled)
      case sources.StringContains(attr, value) =>
        zipAttrAndValue(attr, value).map(Contains.tupled)
      case sources.StringStartsWith(attr, value) =>
        zipAttrAndValue(attr, value).map(StartsWith.tupled)
      case sources.StringEndsWith(attr, value) =>
        zipAttrAndValue(attr, value).map(EndsWith.tupled)
      case sources.AlwaysTrue() =>
        Some(Literal(true, BooleanType))
      case sources.AlwaysFalse() =>
        Some(Literal(false, BooleanType))
      case _ => None
    }

    translate(filter)
  }

  /**
   * Resolves the {@link InternalSchema} from commit metadata if schema evolution
   * is enabled on read.
   *
   * @param spark     active SparkSession
   * @param tablePath base path of the Hudi table
   * @param options   merged table properties and read options
   * @return present Option with InternalSchema if schema evolution is enabled and
   *         schema metadata exists; empty Option otherwise
   */
  def resolveInternalSchema(spark: SparkSession,
                            tablePath: String,
                            options: JMap[String, String]): HOption[InternalSchema] = {
    val optParams = options.asScala.toMap
    if (!HoodieBaseRelation.isSchemaEvolutionEnabledOnRead(optParams, spark)) {
      HOption.empty()
    } else {
      val hadoopConf = spark.sessionState.newHadoopConf()
      val storageConf = HadoopFSUtils.getStorageConf(hadoopConf)
      val metaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf)
        .setBasePath(tablePath)
        .build()

      val schemaResolver = new TableSchemaResolver(metaClient)
      try {
        schemaResolver.getTableInternalSchemaFromCommitMetadata
      } catch {
        case _: Exception => HOption.empty()
      }
    }
  }

  /**
   * Computes comma-separated instant file names from the active timeline.
   * Used for schema evolution configuration in the Hadoop conf.
   *
   * @param spark     active SparkSession
   * @param tablePath base path of the Hudi table
   * @return comma-separated instant file names
   */
  def computeValidCommits(spark: SparkSession,
                          tablePath: String): String = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val storageConf = HadoopFSUtils.getStorageConf(hadoopConf)
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(storageConf)
      .setBasePath(tablePath)
      .build()

    val timeline = metaClient.getActiveTimeline
    val instantFileNameGenerator = metaClient.getTimelineLayout.getInstantFileNameGenerator
    timeline.getInstants.iterator.asScala
      .map(instant => instantFileNameGenerator.getFileName(instant))
      .mkString(",")
  }

  /**
   * Checks whether the given schema supports Spark's vectorized columnar Parquet reads.
   * Delegates to {@code ParquetUtils.isBatchReadSupportedForSchema} which verifies that
   * vectorized reading is enabled and all field types are supported.
   *
   * <p>Must be called on the driver where {@link SQLConf} is available.
   *
   * @param sqlConf the active SQLConf
   * @param schema  the required output schema
   * @return true if columnar batch reads are supported for this schema
   */
  def isColumnarReadSupported(sqlConf: SQLConf, schema: StructType): Boolean = {
    ParquetUtils.isBatchReadSupportedForSchema(sqlConf, schema)
  }
}
