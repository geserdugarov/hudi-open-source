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

import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

/**
 * Schema-evolved reads via the DSv2 path (`format("hudi_v2")`): with
 * `hoodie.schema.on.read.enable` on, the internal schema is resolved from the commit
 * timeline (as of the time-travel instant when one is supplied) and threaded into the
 * columnar file reader; with it off, only Parquet's implicit schema reconciliation
 * applies. The SQL/catalog path with the session conf on keeps routing through
 * `HoodieInternalV2Table`'s V1 fallback (pinned by TestDSv2Fallback), so all reads here
 * use the DataFrame API against the table path.
 *
 * The tables are written through SQL DDL/DML because schema-on-read evolution
 * (ADD/RENAME/ALTER COLUMN) is only reachable through the ALTER TABLE commands. They are
 * partitioned, with the post-evolution insert going to a fresh partition: COW small-file
 * handling would otherwise merge the insert into the existing file group and rewrite the
 * pre-evolution rows under the new schema, leaving no old-schema file to read.
 */
class TestDSv2SchemaEvolution extends HoodieSparkSqlTestBase {

  private val schemaEvolKey = DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.key
  private val asOfKey = DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key

  private def assertBatchScan(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan.toString()
    assert(plan.contains("BatchScan"), s"DSv2 read should plan a BatchScan, got:\n$plan")
  }

  /** Sorted data rows for order-insensitive DSv1-vs-DSv2 comparison. */
  private def sortedRows(df: DataFrame): Seq[Seq[Any]] =
    df.collect().map(_.toSeq).sortBy(_.mkString("|")).toSeq

  private def createPartitionedTable(tableName: String, path: String): Unit = {
    spark.sql(
      s"""CREATE TABLE $tableName (
         |  id INT,
         |  name STRING,
         |  amount INT,
         |  ts LONG,
         |  part STRING
         |) USING hudi
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = 'id',
         |  preCombineField = 'ts'
         |)
         |PARTITIONED BY (part)
         |LOCATION '$path'
       """.stripMargin)
  }

  /** Reads the table path via the given format, with schema.on.read optionally enabled. */
  private def readPath(format: String, path: String, schemaOnRead: Boolean): DataFrame = {
    val reader = spark.read.format(format)
    if (schemaOnRead) {
      reader.option(schemaEvolKey, "true").load(path)
    } else {
      reader.load(path)
    }
  }

  test("Test hudi_v2 reads column add evolution with schema.on.read on and off") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      withSQLConf(schemaEvolKey -> "true") {
        createPartitionedTable(tableName, path)
        spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10, 1000, 'p1'), (2, 'a2', 20, 1000, 'p1')")
        spark.sql(s"ALTER TABLE $tableName ADD COLUMNS (category STRING)")
        spark.sql(s"INSERT INTO $tableName VALUES (3, 'a3', 30, 2000, 'gold', 'p2')")
      }

      // The p1 base file predates the evolution and lacks `category`: rows from it must
      // surface null, the post-evolution p2 row its value — whether the internal schema is
      // threaded (on) or Parquet backfills the missing column itself (off).
      Seq(true, false).foreach { schemaOnRead =>
        val df = readPath("hudi_v2", path, schemaOnRead).select("id", "name", "category")
        assertBatchScan(df)
        checkAnswer(df.orderBy("id").collect())(
          Seq(1, "a1", null),
          Seq(2, "a2", null),
          Seq(3, "a3", "gold")
        )

        val v1Df = readPath("hudi", path, schemaOnRead).select("id", "name", "category")
        assertResult(sortedRows(v1Df))(sortedRows(df))
      }
    }
  }

  test("Test hudi_v2 reads type-promoted column with schema.on.read on and off") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      withSQLConf(schemaEvolKey -> "true") {
        createPartitionedTable(tableName, path)
        spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 100, 1000, 'p1'), (2, 'a2', 200, 1000, 'p1')")
        spark.sql(s"ALTER TABLE $tableName ALTER COLUMN amount TYPE BIGINT")
        // 3000000000 does not fit in INT, so this row requires the promoted type.
        spark.sql(s"INSERT INTO $tableName VALUES (3, 'a3', 3000000000, 2000, 'p2')")
      }

      // The p1 base file stores amount as INT32 while the table schema says LONG. With
      // schema.on.read on, the internal schema drives the conversion; with it off, the
      // reader's implicit type-change reconciliation must produce the same longs.
      Seq(true, false).foreach { schemaOnRead =>
        val df = readPath("hudi_v2", path, schemaOnRead).select("id", "amount")
        assertBatchScan(df)
        checkAnswer(df.orderBy("id").collect())(
          Seq(1, 100L),
          Seq(2, 200L),
          Seq(3, 3000000000L)
        )

        val v1Df = readPath("hudi", path, schemaOnRead).select("id", "amount")
        assertResult(sortedRows(v1Df))(sortedRows(df))
      }
    }
  }

  test("Test hudi_v2 renamed column resolves old files only with schema.on.read on") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      // The rename DDL's schema-compatibility check treats the old column name as dropped;
      // allow it the same way TestSpark3DDL's rename coverage does.
      withSQLConf(schemaEvolKey -> "true",
        "hoodie.datasource.write.schema.allow.auto.evolution.column.drop" -> "true") {
        createPartitionedTable(tableName, path)
        spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10, 1000, 'p1'), (2, 'a2', 20, 1000, 'p1')")
        spark.sql(s"ALTER TABLE $tableName RENAME COLUMN name TO full_name")
        spark.sql(s"INSERT INTO $tableName VALUES (3, 'a3', 30, 2000, 'p2')")
      }

      // The p1 base file persists the column as `name`; only the internal schema's
      // column-ID mapping can resolve `full_name` against it. This is the discriminating
      // case for the internal-schema threading: column adds and type promotions are
      // reconciled by the reader even without it.
      val dfOn = readPath("hudi_v2", path, schemaOnRead = true).select("id", "full_name")
      assertBatchScan(dfOn)
      checkAnswer(dfOn.orderBy("id").collect())(
        Seq(1, "a1"),
        Seq(2, "a2"),
        Seq(3, "a3")
      )

      val v1DfOn = readPath("hudi", path, schemaOnRead = true).select("id", "full_name")
      assertResult(sortedRows(v1DfOn))(sortedRows(dfOn))

      // With schema.on.read off there is no ID mapping: `full_name` cannot be matched to
      // the p1 file's `name` column and pre-rename rows surface null — same as DSv1.
      val dfOff = readPath("hudi_v2", path, schemaOnRead = false).select("id", "full_name")
      assertBatchScan(dfOff)
      checkAnswer(dfOff.orderBy("id").collect())(
        Seq(1, null),
        Seq(2, null),
        Seq(3, "a3")
      )
      val v1DfOff = readPath("hudi", path, schemaOnRead = false).select("id", "full_name")
      assertResult(sortedRows(v1DfOff))(sortedRows(dfOff))
    }
  }

  test("Test hudi_v2 time travel on a schema-evolved table reads the pre-evolution snapshot") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      var firstInstant: String = null
      withSQLConf(schemaEvolKey -> "true") {
        createPartitionedTable(tableName, path)
        spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10, 1000, 'p1'), (2, 'a2', 20, 1000, 'p1')")
        firstInstant = createMetaClient(spark, path).getActiveTimeline
          .getAllCommitsTimeline.lastInstant().get().requestedTime
        spark.sql(s"ALTER TABLE $tableName ADD COLUMNS (region STRING)")
        spark.sql(s"INSERT INTO $tableName VALUES (3, 'a3', 30, 2000, 'us-west', 'p2')")
      }

      // Both the schema and the internal schema must resolve as of the instant: the
      // post-evolution column must not leak into the time-travel schema, and only the
      // first commit's rows may surface.
      val df = spark.read.format("hudi_v2")
        .option(schemaEvolKey, "true")
        .option(asOfKey, firstInstant)
        .load(path)
      assertBatchScan(df)
      assert(!df.schema.fieldNames.contains("region"),
        s"time-travel schema must not expose post-evolution column `region`, " +
          s"got: ${df.schema.fieldNames.mkString(",")}")
      checkAnswer(df.select("id", "name", "amount").orderBy("id").collect())(
        Seq(1, "a1", 10),
        Seq(2, "a2", 20)
      )

      val v1Df = spark.read.format("hudi")
        .option(schemaEvolKey, "true")
        .option(asOfKey, firstInstant)
        .load(path)
        .select("id", "name", "amount")
      assertResult(sortedRows(v1Df))(sortedRows(df.select("id", "name", "amount")))
    }
  }
}
