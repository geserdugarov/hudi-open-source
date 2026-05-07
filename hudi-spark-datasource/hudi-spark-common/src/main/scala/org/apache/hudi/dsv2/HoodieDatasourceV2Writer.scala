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

package org.apache.hudi.dsv2

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{HIVE_SYNC_ENABLED_OPT_KEY, META_SYNC_ENABLED_OPT_KEY}
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.config.{HoodieConfig, TypedProperties}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieRecord, HoodieRecordLocation, WriteOperationType}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.{CommitUtils, Option => HOption, ReflectionUtils}
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.io.storage.row.HoodieUpsertHandle
import org.apache.hudi.keygen.BuiltinKeyGenerator
import org.apache.hudi.table.{WorkloadProfile, WorkloadStat}
import org.apache.hudi.table.action.commit.UpsertPartitioner

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{functions, DataFrame, Row, SaveMode, SQLContext}
import org.apache.spark.sql.functions.{col, struct, udf}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Drives the Spark DataSource V2 write pipeline for COW upserts.
 *
 * Activated from [[org.apache.hudi.HoodieSparkSqlWriter]] when
 * [[HoodieWriteConfig.DATASOURCE_V2_WRITE_ENABLED]] is set. Performs the same
 * three-step shape as the V1 upsert path but keeps old records out of the shuffle:
 *   1. enrich the new DataFrame with record-key, partition-path, and routing columns
 *      pointing at the file group each record will land in;
 *   2. derive a [[WorkloadProfile]] from the join with the existing table's
 *      `_hoodie_*` projection (3 columns, no data scan);
 *   3. hand the enriched DataFrame off to the dsv2 datasource ([[DefaultSource]]),
 *      whose [[BulkInsertDataInternalWriterHelper]] reads each old parquet file
 *      locally inside the write task and merges with buffered new rows.
 */
object HoodieDatasourceV2Writer {

  // Intermediate DataFrame columns used only between prepareForUpsert (producer)
  // and buildWorkloadProfile (consumer). Not persisted, not exposed.
  private val OLD_RECORD_KEY_COL = "_old_rk"
  private val OLD_PARTITION_PATH_COL = "old_partition_path"
  private val NEW_PARTITION_PATH_COL = "new_partition_path"
  private val NUM_RECS_COL = "numRecs"

  /**
   * Run the dsv2 upsert. Returns the same tuple shape as the V1 bulk-insert path so the
   * caller in HoodieSparkSqlWriter can `return` the result without further translation.
   *
   * @param runMetaSync invoked iff hive_sync / meta_sync is enabled in `parameters`;
   *                    bound by the caller because the HoodieSparkSqlWriter `metaSync`
   *                    method is private to that class.
   */
  def write(sqlContext: SQLContext,
            client: SparkRDDWriteClient[_],
            df: DataFrame,
            parameters: Map[String, String],
            hoodieConfig: HoodieConfig,
            writerSchema: HoodieSchema,
            tableConfig: HoodieTableConfig,
            basePath: Path,
            runMetaSync: () => Boolean):
  (Boolean, HOption[String], HOption[String], HOption[String], SparkRDDWriteClient[_], HoodieTableConfig) = {

    val commitActionType = CommitUtils.getCommitActionType(WriteOperationType.UPSERT, tableConfig.getTableType)
    val instantTime = client.startCommit(commitActionType)

    val writeDf = prepareForUpsert(df, sqlContext, tableConfig, client, instantTime)
    writeDf.write.format(DefaultSource.FORMAT_NAME)
      .option(DataSourceInternalWriterHelper.INSTANT_TIME_OPT_KEY, instantTime)
      .option(HoodieWriteConfig.BULKINSERT_INPUT_DATA_SCHEMA_DDL, writeDf.schema.toDDL)
      .options(parameters ++ Map(HoodieWriteConfig.WRITE_SCHEMA_OVERRIDE.key -> writerSchema.toString))
      .mode(SaveMode.Append)
      .save()

    val syncRequested = parameters.get(HIVE_SYNC_ENABLED_OPT_KEY).exists(_.toBoolean) ||
      parameters.get(META_SYNC_ENABLED_OPT_KEY).exists(_.toBoolean)
    val syncSuccess = if (syncRequested) runMetaSync() else true

    (syncSuccess, HOption.ofNullable(instantTime), HOption.empty(), HOption.empty(), client, tableConfig)
  }

  /**
   * Build the enriched DataFrame the dsv2 DataWriter consumes:
   *   - inject `_hoodie_record_key` and `_hoodie_partition_path` via the configured key gen;
   *   - left-join the existing table's `(record_key, partition_path, file_name)` projection
   *     to attach the old file-name to records that update an existing key;
   *   - derive `file_id`, `partition_id`, `file_id_pfx_new` via the [[UpsertPartitioner]];
   *   - repartition + sort by `partition_id` so each task sees one file group at a time
   *     (the writer helper switches handles on group boundaries).
   */
  private def prepareForUpsert(df: DataFrame, sqlContext: SQLContext, tableConfig: HoodieTableConfig,
                               writeClient: SparkRDDWriteClient[_], instantTime: String): DataFrame = {
    val properties = new TypedProperties
    writeClient.getConfig.getProps.forEach((k, v) => properties.put(k, v))
    val keyGeneratorClass = properties.getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY)
    val keyGenerator = ReflectionUtils.loadClass(keyGeneratorClass, properties).asInstanceOf[BuiltinKeyGenerator]
    val keyGenUdf = udf((row: Row) => keyGenerator.getRecordKey(row))
    val partitionGenUdf = udf((row: Row) => keyGenerator.getPartitionPath(row))
    val newDf = df
      .withColumn(HoodieRecord.RECORD_KEY_METADATA_FIELD, keyGenUdf(struct("*")))
      .withColumn(HoodieRecord.PARTITION_PATH_METADATA_FIELD, partitionGenUdf(struct("*")))

    // Project old table to only 3 columns needed for the join — avoids scanning all data columns
    val oldMetaDf = sqlContext.sparkSession.read.table(tableConfig.getTableName)
      .select(
        HoodieRecord.RECORD_KEY_METADATA_FIELD,
        HoodieRecord.PARTITION_PATH_METADATA_FIELD,
        HoodieRecord.FILENAME_METADATA_FIELD)

    // Single left join: gives us file-name for each new record that updates an existing one
    val joinedDf = newDf.join(
      oldMetaDf.select(
        col(HoodieRecord.RECORD_KEY_METADATA_FIELD).as(OLD_RECORD_KEY_COL),
        col(HoodieRecord.PARTITION_PATH_METADATA_FIELD).as(OLD_PARTITION_PATH_COL),
        col(HoodieRecord.FILENAME_METADATA_FIELD)
      ),
      newDf(HoodieRecord.RECORD_KEY_METADATA_FIELD) === col(OLD_RECORD_KEY_COL),
      "left"
    ).drop(OLD_RECORD_KEY_COL)

    // Workload profile from the joined DF (no extra scan)
    val workloadDf = joinedDf
      .groupBy(
        col(HoodieRecord.PARTITION_PATH_METADATA_FIELD).as(NEW_PARTITION_PATH_COL),
        col(OLD_PARTITION_PATH_COL),
        col(HoodieRecord.FILENAME_METADATA_FIELD)
      )
      .agg(functions.count("*").alias(NUM_RECS_COL))

    val workloadProfile = buildWorkloadProfile(workloadDf)

    val table = writeClient.initTable(WriteOperationType.UPSERT, HOption.ofNullable(instantTime))
    val upsertPartitioner = new UpsertPartitioner(
      workloadProfile, writeClient.getEngineContext, table, writeClient.getConfig, WriteOperationType.UPSERT)

    val fileIdUdf = udf((fileName: String) => if (fileName == null) null else FSUtils.getFileId(fileName))
    val partitionUdf = udf((key: String, path: String, fileId: String) => upsertPartitioner.getPartition(key, path, fileId))
    val newFileIdPfxUdf = udf((partition: Integer) => upsertPartitioner.getBucketInfo(partition).getFileIdPrefix)

    // writeDf contains ONLY new records enriched with routing columns — no old data rows in shuffle
    joinedDf
      .withColumn(HoodieUpsertHandle.FILE_ID_COL, fileIdUdf(col(HoodieRecord.FILENAME_METADATA_FIELD)))
      .drop(HoodieRecord.FILENAME_METADATA_FIELD, OLD_PARTITION_PATH_COL)
      .withColumn(HoodieUpsertHandle.PARTITION_ID_COL, partitionUdf(
        col(HoodieRecord.RECORD_KEY_METADATA_FIELD),
        col(HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        col(HoodieUpsertHandle.FILE_ID_COL)
      ))
      .withColumn(HoodieUpsertHandle.FILE_ID_PFX_NEW_COL, newFileIdPfxUdf(col(HoodieUpsertHandle.PARTITION_ID_COL)))
      .repartition(upsertPartitioner.numPartitions(), col(HoodieUpsertHandle.PARTITION_ID_COL))
      .sortWithinPartitions(col(HoodieUpsertHandle.PARTITION_ID_COL))
  }

  private def buildWorkloadProfile(workloadDf: DataFrame): WorkloadProfile = {
    val globalStat = new WorkloadStat()
    val partitionStatMap = new mutable.HashMap[String, WorkloadStat]()
    workloadDf.collectAsList().forEach(row => {
      val fileName = row.getAs[String](HoodieRecord.FILENAME_METADATA_FIELD)
      val numRecs = row.getAs[Long](NUM_RECS_COL)
      val oldPartitionPath = row.getAs[String](OLD_PARTITION_PATH_COL)
      val newPartitionPath = row.getAs[String](NEW_PARTITION_PATH_COL)
      if (oldPartitionPath == null) {
        val insertStat = partitionStatMap.getOrElse(newPartitionPath, new WorkloadStat())
        insertStat.addInserts(numRecs)
        globalStat.addInserts(numRecs)
        partitionStatMap.put(newPartitionPath, insertStat)
      } else {
        val fileId = FSUtils.getFileId(fileName)
        val commitTime = FSUtils.getCommitTime(fileName)
        val location = new HoodieRecordLocation(commitTime, fileId)
        val updateStat = partitionStatMap.getOrElse(oldPartitionPath, new WorkloadStat())
        updateStat.addUpdates(location, numRecs)
        globalStat.addUpdates(location, numRecs)
        partitionStatMap.put(oldPartitionPath, updateStat)
      }
    })
    new WorkloadProfile(Pair.of(partitionStatMap.asJava, globalStat))
  }
}
