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
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Assumptions.assumeTrue

/**
 * Functional tests for incremental and read-optimized queries via the DSv2 path.
 */
@Tag("functional")
class TestDSv2IncrementalRead extends SparkClientFunctionalTestHarness {

  override def conf: SparkConf = conf(getSparkSqlConf)

  @BeforeEach
  def checkSparkVersion(): Unit = {
    assumeTrue(HoodieSparkUtils.gteqSpark3_5,
      "DSv2 read tests require Spark 3.5 or later")
  }

  private def getCompletionTime(path: String): String = {
    val storageConf = new HadoopStorageConfiguration(spark.sessionState.newHadoopConf())
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(path)
      .setConf(storageConf)
      .build()
    metaClient.getActiveTimeline.lastInstant().get().getCompletionTime
  }

  @Test
  def testCowIncrementalBasic(): Unit = {
    val path = basePath() + "/cow_incr_basic"
    val _spark = spark
    import _spark.implicits._

    // Commit 1
    Seq((1, "Alice", 100.0), (2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_basic")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val commit1Time = getCompletionTime(path)

    // Commit 2
    Seq((3, "Charlie", 300.0), (4, "Diana", 400.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_basic")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    // Incremental from after commit1 should return only commit2's data
    val v2Df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1Time)
      .load(path)

    val rows = v2Df.select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    assertEquals(2, rows.length)
    assertEquals((3, "Charlie", 300.0), rows(0))
    assertEquals((4, "Diana", 400.0), rows(1))
  }

  @Test
  def testCowIncrementalWithStartAndEnd(): Unit = {
    val path = basePath() + "/cow_incr_range"
    val _spark = spark
    import _spark.implicits._

    // Commit 1
    Seq((1, "Alice", 100.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_range")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val commit1Time = getCompletionTime(path)

    // Commit 2
    Seq((2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_range")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    val commit2Time = getCompletionTime(path)

    // Commit 3
    Seq((3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_range")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    // Range from commit1 to commit2 should only return commit2's data
    val v2Df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1Time)
      .option(DataSourceReadOptions.END_COMMIT.key, commit2Time)
      .load(path)

    val rows = v2Df.select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    assertEquals(1, rows.length)
    assertEquals((2, "Bob", 200.0), rows(0))
  }

  @Test
  def testCowIncrementalEmptyRange(): Unit = {
    val path = basePath() + "/cow_incr_empty"
    val _spark = spark
    import _spark.implicits._

    // Single commit
    Seq((1, "Alice", 100.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_empty")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val commit1Time = getCompletionTime(path)

    // Incremental from after the only commit should return empty
    val v2Df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1Time)
      .load(path)

    assertEquals(0, v2Df.count())
  }

  @Test
  def testCowIncrementalColumnPruning(): Unit = {
    val path = basePath() + "/cow_incr_col_prune"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_col_prune")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val commit1Time = getCompletionTime(path)

    Seq((2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_col_prune")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    val v2Df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1Time)
      .load(path)
      .select("id", "name")

    assertEquals(2, v2Df.schema.fields.length)
    val rows = v2Df.collect().map(r => (r.getInt(0), r.getString(1)))
    assertEquals(1, rows.length)
    assertEquals((2, "Bob"), rows(0))
  }

  @Test
  def testCowIncrementalVsDsv1(): Unit = {
    val path = basePath() + "/cow_incr_vs_v1"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0), (2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_vs_v1")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val commit1Time = getCompletionTime(path)

    Seq((2, "Bob Updated", 250.0), (3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_vs_v1")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    val v2Rows = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1Time)
      .load(path)
      .select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    val v1Rows = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1Time)
      .load(path)
      .select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    assertEquals(v1Rows.toSeq, v2Rows.toSeq)
  }

  @Test
  def testMorIncrementalWithLogFiles(): Unit = {
    val path = basePath() + "/mor_incr_logs"
    val _spark = spark
    import _spark.implicits._

    val morOptions = Map("hoodie.datasource.write.table.type" -> "MERGE_ON_READ")

    // Commit 1: insert
    Seq((1, "Alice", 100.0), (2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_incr_logs")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val commit1Time = getCompletionTime(path)

    // Commit 2: upsert (creates log files)
    Seq((1, "Alice Updated", 150.0), (3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_incr_logs")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    val v2Df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1Time)
      .load(path)

    val v2Rows = v2Df.select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    // Should contain commit 2's changes: Alice Updated + Charlie
    assertEquals(2, v2Rows.length)
    assertEquals((1, "Alice Updated", 150.0), v2Rows(0))
    assertEquals((3, "Charlie", 300.0), v2Rows(1))
  }

  @Test
  def testMorIncrementalVsDsv1(): Unit = {
    val path = basePath() + "/mor_incr_vs_v1"
    val _spark = spark
    import _spark.implicits._

    val morOptions = Map("hoodie.datasource.write.table.type" -> "MERGE_ON_READ")

    Seq((1, "Alice", 100.0), (2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_incr_vs_v1")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val commit1Time = getCompletionTime(path)

    Seq((1, "Alice V2", 150.0), (3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_incr_vs_v1")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    val v2Rows = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1Time)
      .load(path)
      .select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    val v1Rows = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1Time)
      .load(path)
      .select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    assertEquals(v1Rows.toSeq, v2Rows.toSeq)
  }

  @Test
  def testReadOptimizedMor(): Unit = {
    val path = basePath() + "/mor_read_optimized"
    val _spark = spark
    import _spark.implicits._

    val morOptions = Map("hoodie.datasource.write.table.type" -> "MERGE_ON_READ")

    // Insert base files
    Seq((1, "Alice", 100.0), (2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_read_optimized")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    // Upsert to create log files
    Seq((1, "Alice Updated", 150.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_read_optimized")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    // read_optimized should return base file data only (pre-update)
    val roDf = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(path)

    val roRows = roDf.select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    // Should still show old Alice (base file only, no log merge)
    assertEquals(2, roRows.length)
    assertEquals((1, "Alice", 100.0), roRows(0))
    assertEquals((2, "Bob", 200.0), roRows(1))

    // Snapshot should show the updated data
    val snapshotDf = spark.read.format("hudi_v2").load(path)
    val snapshotRows = snapshotDf.select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    assertEquals((1, "Alice Updated", 150.0), snapshotRows(0))
  }

  @Test
  def testIncrementalMissingStartCommit(): Unit = {
    val path = basePath() + "/cow_incr_missing_start"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_missing_start")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    try {
      spark.read.format("hudi_v2")
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .load(path)
        .count()
      assertTrue(false, "Should have thrown HoodieException")
    } catch {
      case e: Exception =>
        assertTrue(e.getMessage.contains(DataSourceReadOptions.START_COMMIT.key) ||
          e.getCause != null && e.getCause.getMessage.contains(DataSourceReadOptions.START_COMMIT.key),
          s"Expected error about missing start commit, got: ${e.getMessage}")
    }
  }

  @Test
  def testCowIncrementalPartitioned(): Unit = {
    val path = basePath() + "/cow_incr_partitioned"
    val _spark = spark
    import _spark.implicits._

    // Commit 1
    Seq((1, "Alice", "US"), (2, "Bob", "UK"))
      .toDF("id", "name", "country")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_partitioned")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "country")
      .mode(SaveMode.Overwrite)
      .save(path)

    val commit1Time = getCompletionTime(path)

    // Commit 2: add records to both partitions
    Seq((3, "Charlie", "US"), (4, "Diana", "UK"))
      .toDF("id", "name", "country")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incr_partitioned")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "country")
      .mode(SaveMode.Append)
      .save(path)

    // Incremental should return only commit 2's records
    val v2Df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1Time)
      .load(path)

    val rows = v2Df.select("id", "name", "country").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getString(2))).sortBy(_._1)

    assertEquals(2, rows.length)
    assertEquals((3, "Charlie", "US"), rows(0))
    assertEquals((4, "Diana", "UK"), rows(1))
  }
}
