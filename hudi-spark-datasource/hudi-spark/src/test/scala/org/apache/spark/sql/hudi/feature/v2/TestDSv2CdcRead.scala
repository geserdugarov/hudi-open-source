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

package org.apache.spark.sql.hudi.feature.v2

import org.apache.hudi.{DataSourceReadOptions, HoodieSparkUtils}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.cdc.HoodieCDCUtils
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{BeforeEach, Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Assumptions.assumeTrue

/**
 * Functional tests for CDC queries via the DSv2 path.
 */
@Tag("functional")
class TestDSv2CdcRead extends SparkClientFunctionalTestHarness {

  override def conf: SparkConf = conf(getSparkSqlConf)

  @BeforeEach
  def checkSparkVersion(): Unit = {
    assumeTrue(HoodieSparkUtils.gteqSpark3_5,
      "DSv2 read tests require Spark 3.5 or later")
  }

  private val cdcOptions: Map[String, String] = Map(
    "hoodie.table.cdc.enabled" -> "true",
    "hoodie.table.cdc.supplemental.logging.mode" -> "DATA_BEFORE_AFTER"
  )

  private def getCompletionTime(path: String): String = {
    val storageConf = new HadoopStorageConfiguration(spark.sessionState.newHadoopConf())
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(path)
      .setConf(storageConf)
      .build()
    metaClient.getActiveTimeline.lastInstant().get().getCompletionTime
  }

  @Test
  def testCdcInsertOnly(): Unit = {
    val path = basePath() + "/cdc_insert_only"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0), (2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(cdcOptions)
      .option("hoodie.table.name", "cdc_insert_only")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val storageConf = new HadoopStorageConfiguration(spark.sessionState.newHadoopConf())
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(path)
      .setConf(storageConf)
      .build()
    val firstInstant = metaClient.getActiveTimeline.firstInstant().get()

    // Use a time before the first instant as start
    val v2Df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.INCREMENTAL_FORMAT.key, DataSourceReadOptions.INCREMENTAL_FORMAT_CDC_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, "0")
      .load(path)

    val rows = v2Df.collect()
    assertTrue(rows.length >= 2, s"Expected at least 2 CDC rows, got ${rows.length}")

    // All should be INSERT operations
    val ops = rows.map(r => r.getString(r.fieldIndex(HoodieCDCUtils.CDC_OPERATION_TYPE)))
    assertTrue(ops.forall(_ == "i"), s"Expected all INSERT ops, got: ${ops.mkString(", ")}")
  }

  @Test
  def testCdcWithUpdates(): Unit = {
    val path = basePath() + "/cdc_updates"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0), (2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(cdcOptions)
      .option("hoodie.table.name", "cdc_updates")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val commit1Time = getCompletionTime(path)

    // Update Alice
    Seq((1, "Alice Updated", 150.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(cdcOptions)
      .option("hoodie.table.name", "cdc_updates")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    // CDC from commit1 should show the update
    val v2Df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.INCREMENTAL_FORMAT.key, DataSourceReadOptions.INCREMENTAL_FORMAT_CDC_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1Time)
      .load(path)

    val rows = v2Df.collect()
    assertTrue(rows.nonEmpty, "Expected CDC records for update")
  }

  @Test
  def testCdcSchema(): Unit = {
    val path = basePath() + "/cdc_schema"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(cdcOptions)
      .option("hoodie.table.name", "cdc_schema")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val v2Df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.INCREMENTAL_FORMAT.key, DataSourceReadOptions.INCREMENTAL_FORMAT_CDC_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, "0")
      .load(path)

    val schema = v2Df.schema
    // CDC schema should have: op, ts_ms, before, after
    assertEquals(4, schema.fields.length)
    assertTrue(schema.fieldNames.contains(HoodieCDCUtils.CDC_OPERATION_TYPE))
    assertTrue(schema.fieldNames.contains(HoodieCDCUtils.CDC_COMMIT_TIMESTAMP))
    assertTrue(schema.fieldNames.contains(HoodieCDCUtils.CDC_BEFORE_IMAGE))
    assertTrue(schema.fieldNames.contains(HoodieCDCUtils.CDC_AFTER_IMAGE))
  }

  @Test
  def testCdcVsDsv1(): Unit = {
    val path = basePath() + "/cdc_vs_v1"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0), (2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(cdcOptions)
      .option("hoodie.table.name", "cdc_vs_v1")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val commit1Time = getCompletionTime(path)

    Seq((1, "Alice Updated", 150.0), (3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(cdcOptions)
      .option("hoodie.table.name", "cdc_vs_v1")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    val v2Count = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.INCREMENTAL_FORMAT.key, DataSourceReadOptions.INCREMENTAL_FORMAT_CDC_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1Time)
      .load(path)
      .count()

    val v1Count = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.INCREMENTAL_FORMAT.key, DataSourceReadOptions.INCREMENTAL_FORMAT_CDC_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1Time)
      .load(path)
      .count()

    assertEquals(v1Count, v2Count, "CDC record count should match between DSv1 and DSv2")
  }

  @Test
  def testCdcMorTable(): Unit = {
    val path = basePath() + "/cdc_mor"
    val _spark = spark
    import _spark.implicits._

    val morCdcOptions = cdcOptions + ("hoodie.datasource.write.table.type" -> "MERGE_ON_READ")

    Seq((1, "Alice", 100.0), (2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morCdcOptions)
      .option("hoodie.table.name", "cdc_mor")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val commit1Time = getCompletionTime(path)

    Seq((1, "Alice Updated", 150.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morCdcOptions)
      .option("hoodie.table.name", "cdc_mor")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    val v2Df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.INCREMENTAL_FORMAT.key, DataSourceReadOptions.INCREMENTAL_FORMAT_CDC_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1Time)
      .load(path)

    val rows = v2Df.collect()
    assertTrue(rows.nonEmpty, "Expected CDC records for MoR table update")
  }
}
