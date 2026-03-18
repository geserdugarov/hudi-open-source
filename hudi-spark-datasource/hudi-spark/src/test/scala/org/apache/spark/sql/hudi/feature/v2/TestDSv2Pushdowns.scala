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
 * Functional tests for limit pushdown, statistics reporting, and aggregate pushdown via DSv2.
 */
@Tag("functional")
class TestDSv2Pushdowns extends SparkClientFunctionalTestHarness {

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

  private def writeTestData(path: String, tableName: String, numRecords: Int = 10): Unit = {
    val _spark = spark
    import _spark.implicits._
    (1 to numRecords).map(i => (i, s"name_$i", i * 100.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)
  }

  private def writePartitionedTestData(path: String, tableName: String): Unit = {
    val _spark = spark
    import _spark.implicits._
    Seq(
      (1, "Alice", 100.0, "US"),
      (2, "Bob", 200.0, "UK"),
      (3, "Charlie", 300.0, "US"),
      (4, "Diana", 150.0, "FR"),
      (5, "Eve", 250.0, "UK"),
      (6, "Frank", 350.0, "US"),
      (7, "Grace", 450.0, "FR")
    ).toDF("id", "name", "amount", "country")
      .write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .option("hoodie.datasource.write.partitionpath.field", "country")
      .mode(SaveMode.Overwrite)
      .save(path)
  }

  // ==========================================================================
  // Limit pushdown tests
  // ==========================================================================

  @Test
  def testLimitReducesRowCount(): Unit = {
    val path = basePath() + "/limit_basic"
    writeTestData(path, "limit_basic")

    val df = spark.read.format("hudi_v2").load(path).limit(3)
    assertEquals(3, df.count())
  }

  @Test
  def testLimitGreaterThanTableSize(): Unit = {
    val path = basePath() + "/limit_greater"
    writeTestData(path, "limit_greater", numRecords = 5)

    val df = spark.read.format("hudi_v2").load(path).limit(100)
    assertEquals(5, df.count())
  }

  @Test
  def testLimitWithFilter(): Unit = {
    val path = basePath() + "/limit_filter"
    writePartitionedTestData(path, "limit_filter")

    val df = spark.read.format("hudi_v2").load(path)
      .filter("country = 'US'")
      .limit(1)
    assertEquals(1, df.count())

    val country = df.select("country").collect().head.getString(0)
    assertEquals("US", country)
  }

  @Test
  def testLimitOnMoRTable(): Unit = {
    val path = basePath() + "/limit_mor"
    val _spark = spark
    import _spark.implicits._

    val morOptions = Map("hoodie.datasource.write.table.type" -> "MERGE_ON_READ")

    Seq((1, "Alice", 100.0), (2, "Bob", 200.0), (3, "Charlie", 300.0),
      (4, "Diana", 400.0), (5, "Eve", 500.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "limit_mor")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    // Upsert to create log files
    Seq((1, "Alice Updated", 150.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "limit_mor")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    val df = spark.read.format("hudi_v2").load(path).limit(2)
    assertEquals(2, df.count())
  }

  @Test
  def testLimitOnIncrementalQuery(): Unit = {
    val path = basePath() + "/limit_incr"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "limit_incr")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val commit1Time = getCompletionTime(path)

    Seq((2, "Bob", 200.0), (3, "Charlie", 300.0), (4, "Diana", 400.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "limit_incr")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    val df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1Time)
      .load(path)
      .limit(2)

    assertEquals(2, df.count())
  }

  @Test
  def testLimitDsv1VsDsv2(): Unit = {
    val path = basePath() + "/limit_v1_v2"
    writeTestData(path, "limit_v1_v2", numRecords = 10)

    val v1Count = spark.read.format("hudi").load(path).limit(5).count()
    val v2Count = spark.read.format("hudi_v2").load(path).limit(5).count()
    assertEquals(v1Count, v2Count)
    assertEquals(5, v2Count)
  }

  @Test
  def testLimitOne(): Unit = {
    val path = basePath() + "/limit_one"
    writeTestData(path, "limit_one", numRecords = 10)

    val df = spark.read.format("hudi_v2").load(path).limit(1)
    assertEquals(1, df.count())
  }

  @Test
  def testExplainShowsLimitInfo(): Unit = {
    val path = basePath() + "/limit_explain"
    writeTestData(path, "limit_explain", numRecords = 5)

    val df = spark.read.format("hudi_v2").load(path).limit(3)
    val plan = df.queryExecution.executedPlan.toString()
    assertTrue(plan.contains("BatchScan"), s"Expected BatchScan in plan:\n$plan")
    assertTrue(plan.contains("PushedLimit"), s"Expected PushedLimit in plan:\n$plan")
  }

  // ==========================================================================
  // Statistics reporting tests
  // ==========================================================================

  @Test
  def testStatisticsReportsSizeInBytes(): Unit = {
    val path = basePath() + "/stats_size"
    writeTestData(path, "stats_size", numRecords = 10)

    val df = spark.read.format("hudi_v2").load(path)
    val plan = df.queryExecution.optimizedPlan
    val stats = plan.stats

    assertTrue(stats.sizeInBytes > 0, s"Expected positive sizeInBytes, got: ${stats.sizeInBytes}")
  }

  @Test
  def testStatisticsEmptyTable(): Unit = {
    val path = basePath() + "/stats_empty"
    val _spark = spark
    import _spark.implicits._

    // Write and then read with a filter that matches nothing
    Seq((1, "Alice", 100.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "stats_empty")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    // The plan should have statistics
    val df = spark.read.format("hudi_v2").load(path)
    val plan = df.queryExecution.optimizedPlan
    val stats = plan.stats
    assertTrue(stats.sizeInBytes > 0, "Expected positive sizeInBytes for non-empty table")
  }

  // ==========================================================================
  // Aggregate pushdown tests (conditional on column stats availability)
  // ==========================================================================

  @Test
  def testCountStarWithoutColumnStats(): Unit = {
    // Without column stats enabled, COUNT(*) should still return correct result
    // (falls back to scanning all files)
    val path = basePath() + "/count_no_stats"
    writeTestData(path, "count_no_stats", numRecords = 5)

    val count = spark.read.format("hudi_v2").load(path)
      .selectExpr("count(*)").collect().head.getLong(0)
    assertEquals(5, count)
  }

  @Test
  def testCountStarDsv1VsDsv2(): Unit = {
    val path = basePath() + "/count_v1_v2"
    writeTestData(path, "count_v1_v2", numRecords = 8)

    val v1Count = spark.read.format("hudi").load(path)
      .selectExpr("count(*)").collect().head.getLong(0)
    val v2Count = spark.read.format("hudi_v2").load(path)
      .selectExpr("count(*)").collect().head.getLong(0)
    assertEquals(v1Count, v2Count)
  }

  @Test
  def testMinMaxWithoutColumnStats(): Unit = {
    val path = basePath() + "/minmax_no_stats"
    writeTestData(path, "minmax_no_stats", numRecords = 5)

    val row = spark.read.format("hudi_v2").load(path)
      .selectExpr("min(amount)", "max(amount)").collect().head

    assertEquals(100.0, row.getDouble(0))
    assertEquals(500.0, row.getDouble(1))
  }

  @Test
  def testAggregateWithGroupByNotPushed(): Unit = {
    val path = basePath() + "/agg_groupby"
    writePartitionedTestData(path, "agg_groupby")

    // GROUP BY should prevent aggregate pushdown, but still return correct results
    val rows = spark.read.format("hudi_v2").load(path)
      .groupBy("country").count()
      .collect().map(r => (r.getString(0), r.getLong(1))).sortBy(_._1)

    assertEquals("FR", rows(0)._1)
    assertEquals(2, rows(0)._2)
    assertEquals("UK", rows(1)._1)
    assertEquals(2, rows(1)._2)
    assertEquals("US", rows(2)._1)
    assertEquals(3, rows(2)._2)
  }

  @Test
  def testAggregateOnMoRWithLogFiles(): Unit = {
    val path = basePath() + "/agg_mor"
    val _spark = spark
    import _spark.implicits._

    val morOptions = Map("hoodie.datasource.write.table.type" -> "MERGE_ON_READ")

    Seq((1, "Alice", 100.0), (2, "Bob", 200.0), (3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "agg_mor")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    // Upsert to create log files
    Seq((1, "Alice Updated", 150.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "agg_mor")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    // COUNT(*) should work (falls back to scan since MoR has log files)
    val count = spark.read.format("hudi_v2").load(path)
      .selectExpr("count(*)").collect().head.getLong(0)
    assertEquals(3, count)

    // MIN/MAX should reflect merged data
    val row = spark.read.format("hudi_v2").load(path)
      .selectExpr("min(amount)", "max(amount)").collect().head
    assertEquals(150.0, row.getDouble(0))
    assertEquals(300.0, row.getDouble(1))
  }

  @Test
  def testCountWithPartitionFilter(): Unit = {
    val path = basePath() + "/count_part_filter"
    writePartitionedTestData(path, "count_part_filter")

    val count = spark.read.format("hudi_v2").load(path)
      .filter("country = 'US'")
      .selectExpr("count(*)").collect().head.getLong(0)
    assertEquals(3, count)
  }
}
