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
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{BeforeEach, Tag, Test}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assumptions.assumeTrue

/**
 * Functional tests for MoR snapshot reading via the DSv2 path.
 */
@Tag("functional")
class TestDSv2MorSnapshotRead extends SparkClientFunctionalTestHarness {

  override def conf: SparkConf = conf(getSparkSqlConf)

  @BeforeEach
  def checkSparkVersion(): Unit = {
    assumeTrue(HoodieSparkUtils.gteqSpark3_5,
      "DSv2 read tests require Spark 3.5 or later")
  }

  private val morOptions: Map[String, String] = Map(
    "hoodie.datasource.write.table.type" -> "MERGE_ON_READ"
  )

  @Test
  def testMorReadBaseOnlyViaDataFrameApi(): Unit = {
    val path = basePath() + "/mor_base_only"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0), (2, "Bob", 200.0), (3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_base_only")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val v2Df = spark.read.format("hudi_v2").load(path)
    assertEquals(3, v2Df.count())

    val v2Rows = v2Df.select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    val v1Rows = spark.read.format("hudi").load(path)
      .select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    assertEquals(v1Rows.toSeq, v2Rows.toSeq)
  }

  @Test
  def testMorReadWithLogFilesViaDataFrameApi(): Unit = {
    val path = basePath() + "/mor_with_logs"
    val _spark = spark
    import _spark.implicits._

    // Initial insert (creates base files)
    Seq((1, "Alice", 100.0), (2, "Bob", 200.0), (3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_with_logs")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    // Upsert (creates log files)
    Seq((1, "Alice Updated", 150.0), (4, "Diana", 400.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_with_logs")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    val v2Df = spark.read.format("hudi_v2").load(path)
    val v2Rows = v2Df.select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    val v1Rows = spark.read.format("hudi").load(path)
      .select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    assertEquals(v1Rows.toSeq, v2Rows.toSeq)
    assertEquals(4, v2Rows.length)
    assertEquals((1, "Alice Updated", 150.0), v2Rows(0))
  }

  @Test
  def testMorReadViaSqlCatalog(): Unit = {
    val tableName = "mor_sql_read"
    val tablePath = basePath() + "/" + tableName
    spark.sql(
      s"""CREATE TABLE $tableName (
         |  id INT,
         |  name STRING,
         |  amount DOUBLE,
         |  ts LONG
         |) USING hudi
         |TBLPROPERTIES (
         |  type = 'mor',
         |  primaryKey = 'id',
         |  orderingFields = 'ts'
         |)
         |LOCATION '$tablePath'
         """.stripMargin)

    spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 100.0, 1), (2, 'Bob', 200.0, 2), (3, 'Charlie', 300.0, 3)")
    spark.sql(s"UPDATE $tableName SET name = 'Alice Updated', amount = 150.0, ts = 4 WHERE id = 1")

    spark.sessionState.conf.setConfString(DataSourceReadOptions.USE_V2_READ.key, "true")
    try {
      val v2Df = spark.sql(s"SELECT id, name, amount FROM $tableName")
      val v2Rows = v2Df.collect()
        .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

      spark.sessionState.conf.setConfString(DataSourceReadOptions.USE_V2_READ.key, "false")
      val v1Df = spark.sql(s"SELECT id, name, amount FROM $tableName")
      val v1Rows = v1Df.collect()
        .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

      assertEquals(v1Rows.toSeq, v2Rows.toSeq)
      assertEquals(3, v2Rows.length)
      assertEquals((1, "Alice Updated", 150.0), v2Rows(0))
    } finally {
      spark.sessionState.conf.unsetConf(DataSourceReadOptions.USE_V2_READ.key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testMorWithDeletes(): Unit = {
    val path = basePath() + "/mor_deletes"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0), (2, "Bob", 200.0), (3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_deletes")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    // Delete record with id=2
    Seq((2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_deletes")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .option("hoodie.datasource.write.operation", "delete")
      .mode(SaveMode.Append)
      .save(path)

    val v2Df = spark.read.format("hudi_v2").load(path)
    val v2Rows = v2Df.select("id", "name").collect()
      .map(r => (r.getInt(0), r.getString(1))).sortBy(_._1)

    val v1Rows = spark.read.format("hudi").load(path)
      .select("id", "name").collect()
      .map(r => (r.getInt(0), r.getString(1))).sortBy(_._1)

    assertEquals(v1Rows.toSeq, v2Rows.toSeq)
    assertEquals(2, v2Rows.length)
  }

  @Test
  def testMorColumnPruning(): Unit = {
    val path = basePath() + "/mor_col_pruning"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0), (2, "Bob", 200.0), (3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_col_pruning")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    // Upsert to create log files
    Seq((1, "Alice Updated", 150.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_col_pruning")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    val df = spark.read.format("hudi_v2").load(path).select("id", "name")
    assertEquals(2, df.schema.fields.length)
    assertEquals(3, df.count())

    val rows = df.collect().map(r => (r.getInt(0), r.getString(1))).sortBy(_._1)
    assertEquals(Seq((1, "Alice Updated"), (2, "Bob"), (3, "Charlie")), rows.toSeq)
  }

  @Test
  def testMorPartitionedTable(): Unit = {
    val path = basePath() + "/mor_partitioned"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", "US"), (2, "Bob", "UK"), (3, "Charlie", "US"))
      .toDF("id", "name", "country")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_partitioned")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "country")
      .mode(SaveMode.Overwrite)
      .save(path)

    // Upsert across partitions
    Seq((1, "Alice Updated", "US"), (4, "Diana", "UK"))
      .toDF("id", "name", "country")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_partitioned")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "country")
      .mode(SaveMode.Append)
      .save(path)

    val v2Df = spark.read.format("hudi_v2").load(path)
    val v2Rows = v2Df.select("id", "name", "country").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getString(2))).sortBy(_._1)

    val v1Rows = spark.read.format("hudi").load(path)
      .select("id", "name", "country").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getString(2))).sortBy(_._1)

    assertEquals(v1Rows.toSeq, v2Rows.toSeq)
    assertEquals(4, v2Rows.length)
    assertEquals((1, "Alice Updated", "US"), v2Rows(0))
  }

  @Test
  def testMorDsv1VsDsv2ResultComparison(): Unit = {
    val path = basePath() + "/mor_v1_v2_compare"
    val _spark = spark
    import _spark.implicits._

    // Insert
    Seq((1, "Alice", 100.0), (2, "Bob", 200.0), (3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_v1_v2_compare")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    // Update
    Seq((1, "Alice Updated", 150.0), (3, "Charlie Updated", 350.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_v1_v2_compare")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    // Delete
    Seq((2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_v1_v2_compare")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .option("hoodie.datasource.write.operation", "delete")
      .mode(SaveMode.Append)
      .save(path)

    val v1Rows = spark.read.format("hudi").load(path)
      .select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    val v2Rows = spark.read.format("hudi_v2").load(path)
      .select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    assertEquals(v1Rows.toSeq, v2Rows.toSeq)
  }

  @Test
  def testMorWithMultipleLogFiles(): Unit = {
    val path = basePath() + "/mor_multi_logs"
    val _spark = spark
    import _spark.implicits._

    // Initial insert
    Seq((1, "Alice", 100.0), (2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_multi_logs")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    // First upsert
    Seq((1, "Alice V2", 150.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_multi_logs")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    // Second upsert
    Seq((1, "Alice V3", 175.0), (3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_multi_logs")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    // Third upsert
    Seq((2, "Bob V2", 250.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .options(morOptions)
      .option("hoodie.table.name", "mor_multi_logs")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    val v2Rows = spark.read.format("hudi_v2").load(path)
      .select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    val v1Rows = spark.read.format("hudi").load(path)
      .select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)

    assertEquals(v1Rows.toSeq, v2Rows.toSeq)
    assertEquals(3, v2Rows.length)
    assertEquals((1, "Alice V3", 175.0), v2Rows(0))
    assertEquals((2, "Bob V2", 250.0), v2Rows(1))
    assertEquals((3, "Charlie", 300.0), v2Rows(2))
  }
}
