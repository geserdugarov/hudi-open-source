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
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{BeforeEach, Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Assumptions.assumeTrue

import scala.collection.JavaConverters._

/**
 * Functional tests for incremental reading via the DSv2 path.
 */
@Tag("functional")
class TestDSv2IncrementalRead extends SparkClientFunctionalTestHarness {

  override def conf: SparkConf = conf(getSparkSqlConf)

  @BeforeEach
  def checkSparkVersion(): Unit = {
    assumeTrue(HoodieSparkUtils.gteqSpark3_5,
      "DSv2 read tests require Spark 3.5 or later")
  }

  private val morOptions: Map[String, String] = Map(
    "hoodie.datasource.write.table.type" -> "MERGE_ON_READ"
  )

  private def getCompletionTimes(path: String): Seq[String] = {
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(path)
      .setConf(new HadoopStorageConfiguration(spark.sessionState.newHadoopConf()))
      .build()
    metaClient.reloadActiveTimeline()
      .getCommitsTimeline.filterCompletedInstants
      .getInstantsAsStream.iterator().asScala
      .map(_.getCompletionTime).toSeq
  }

  private def getRequestedTimes(path: String): Seq[String] = {
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(path)
      .setConf(new HadoopStorageConfiguration(spark.sessionState.newHadoopConf()))
      .build()
    metaClient.reloadActiveTimeline()
      .getCommitsTimeline.filterCompletedInstants
      .getInstantsAsStream.iterator().asScala
      .map(_.requestedTime).toSeq
  }

  @Test
  def testCowIncrementalBasic(): Unit = {
    val path = basePath() + "/cow_incr_basic"
    val _spark = spark
    import _spark.implicits._

    // Batch 1
    Seq((1, "Alice", 100.0), (2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_basic")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val commit1CompletionTime = getCompletionTimes(path).head

    // Batch 2
    Seq((3, "Charlie", 300.0), (1, "Alice Updated", 150.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_basic")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    // Incremental read from after commit1
    val df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1CompletionTime)
      .load(path)

    val rows = df.select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    assertEquals(2, rows.length)
    assertEquals((1, "Alice Updated", 150.0), rows(0))
    assertEquals((3, "Charlie", 300.0), rows(1))
  }

  @Test
  def testCowIncrementalStartAndEndRange(): Unit = {
    val path = basePath() + "/cow_incr_range"
    val _spark = spark
    import _spark.implicits._

    // Batch 1
    Seq((1, "Alice", 100.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_range")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val commit1CompletionTime = getCompletionTimes(path).head

    // Batch 2
    Seq((2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_range")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    val commit2CompletionTime = getCompletionTimes(path).last

    // Batch 3
    Seq((3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_range")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    // Read only commit2 (between commit1 and commit2 completion times)
    val df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1CompletionTime)
      .option(DataSourceReadOptions.END_COMMIT.key, commit2CompletionTime)
      .load(path)

    val rows = df.select("id", "name").collect()
      .map(r => (r.getInt(0), r.getString(1))).sortBy(_._1)

    assertEquals(1, rows.length)
    assertEquals((2, "Bob"), rows(0))
  }

  @Test
  def testCowIncrementalEmptyRange(): Unit = {
    val path = basePath() + "/cow_incr_empty"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_empty")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val latestCompletionTime = getCompletionTimes(path).last

    // Start after the latest commit - should return empty
    val df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, latestCompletionTime)
      .load(path)

    assertEquals(0, df.count())
  }

  @Test
  def testCowIncrementalColumnPruning(): Unit = {
    val path = basePath() + "/cow_incr_col_prune"
    val _spark = spark
    import _spark.implicits._

    // Batch 1
    Seq((1, "Alice", 100.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_col_prune")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val commit1CompletionTime = getCompletionTimes(path).head

    // Batch 2
    Seq((2, "Bob", 200.0), (3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_col_prune")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    // Select only "id" column
    val df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1CompletionTime)
      .load(path)
      .select("id")

    assertEquals(1, df.schema.fields.length)
    val ids = df.collect().map(_.getInt(0)).sorted
    assertEquals(Seq(2, 3), ids.toSeq)
  }

  @Test
  def testCowIncrementalVsDsv1Parity(): Unit = {
    val path = basePath() + "/cow_incr_parity"
    val _spark = spark
    import _spark.implicits._

    // Batch 1
    Seq((1, "Alice", 100.0), (2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_parity")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val commit1CompletionTime = getCompletionTimes(path).head

    // Batch 2
    Seq((3, "Charlie", 300.0), (1, "Alice Updated", 150.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_parity")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    // DSv2 incremental
    val v2Df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1CompletionTime)
      .load(path)
      .select("id", "name", "amount")

    // DSv1 incremental
    val v1Df = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1CompletionTime)
      .load(path)
      .select("id", "name", "amount")

    val v2Rows = v2Df.collect().map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)
    val v1Rows = v1Df.collect().map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    assertEquals(v1Rows.toSeq, v2Rows.toSeq)
  }

  @Test
  def testMorIncrementalWithLogFiles(): Unit = {
    val path = basePath() + "/mor_incr_logs"
    val _spark = spark
    import _spark.implicits._

    // Batch 1: insert (creates base files)
    Seq((1, "Alice", 100.0), (2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_incr_logs")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val commit1CompletionTime = getCompletionTimes(path).head

    // Batch 2: upsert (creates log files)
    Seq((1, "Alice Updated", 150.0), (3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_incr_logs")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    val df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1CompletionTime)
      .load(path)

    val rows = df.select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    assertEquals(2, rows.length)
    assertEquals((1, "Alice Updated", 150.0), rows(0))
    assertEquals((3, "Charlie", 300.0), rows(1))
  }

  @Test
  def testMorIncrementalVsDsv1(): Unit = {
    val path = basePath() + "/mor_incr_parity"
    val _spark = spark
    import _spark.implicits._

    // Batch 1
    Seq((1, "Alice", 100.0), (2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_incr_parity")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val commit1CompletionTime = getCompletionTimes(path).head

    // Batch 2
    Seq((1, "Alice Updated", 150.0), (3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_incr_parity")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    // DSv2
    val v2Df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1CompletionTime)
      .load(path)
      .select("id", "name", "amount")

    // DSv1
    val v1Df = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1CompletionTime)
      .load(path)
      .select("id", "name", "amount")

    val v2Rows = v2Df.collect().map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)
    val v1Rows = v1Df.collect().map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    assertEquals(v1Rows.toSeq, v2Rows.toSeq)
  }

  @Test
  def testReadOptimizedMor(): Unit = {
    val path = basePath() + "/mor_read_optimized"
    val _spark = spark
    import _spark.implicits._

    // Batch 1: insert (creates base files)
    Seq((1, "Alice", 100.0), (2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_read_optimized")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    // Batch 2: upsert (creates log files - should not be seen in read_optimized)
    Seq((1, "Alice Updated", 150.0), (3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_read_optimized")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    // DSv2 read_optimized
    val v2Df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(path)

    // DSv1 read_optimized
    val v1Df = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(path)

    val v2Rows = v2Df.select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)
    val v1Rows = v1Df.select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    assertEquals(v1Rows.toSeq, v2Rows.toSeq)
  }

  @Test
  def testMissingStartCommitError(): Unit = {
    val path = basePath() + "/cow_no_start"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_no_start")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    assertThrows(classOf[Exception], () => {
      spark.read.format("hudi_v2")
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .load(path)
        .collect()
    })
  }

  @Test
  def testPartitionedIncremental(): Unit = {
    val path = basePath() + "/cow_incr_partitioned"
    val _spark = spark
    import _spark.implicits._

    // Batch 1
    Seq((1, "Alice", "US"), (2, "Bob", "UK"))
      .toDF("id", "name", "country")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_partitioned")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "country")
      .mode(SaveMode.Overwrite)
      .save(path)

    val commit1CompletionTime = getCompletionTimes(path).head

    // Batch 2
    Seq((3, "Charlie", "US"), (4, "Diana", "DE"))
      .toDF("id", "name", "country")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_partitioned")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "country")
      .mode(SaveMode.Append)
      .save(path)

    val df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1CompletionTime)
      .load(path)

    val rows = df.select("id", "name", "country").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getString(2))).sortBy(_._1)

    assertEquals(2, rows.length)
    assertEquals((3, "Charlie", "US"), rows(0))
    assertEquals((4, "Diana", "DE"), rows(1))
  }
}
