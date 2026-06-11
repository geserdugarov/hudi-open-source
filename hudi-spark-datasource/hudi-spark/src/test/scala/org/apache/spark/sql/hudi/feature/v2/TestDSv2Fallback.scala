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

import org.apache.hudi.{DataSourceReadOptions, DefaultSparkRecordMerger}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

/**
 * SQL/catalog fallback matrix for the DSv2 read opt-in (`hoodie.datasource.read.use.v2`):
 * supported queries must resolve to a DSv2 `BatchScan`, while everything the gate
 * ([[org.apache.spark.sql.hudi.v2.HoodieV2ReadSupport.isSupportedByDSv2]]) rejects must
 * silently stay on the V1 read path with semantics identical to the flag being off.
 *
 * The DSv2 scan is still the empty skeleton, so DSv2-routed cases assert plan shape only
 * (`BatchScan` in EXPLAIN); V1-fallback cases additionally assert row contents.
 */
class TestDSv2Fallback extends HoodieSparkSqlTestBase {

  private val useV2Key = DataSourceReadOptions.USE_V2_READ.key
  private val queryTypeKey = DataSourceReadOptions.QUERY_TYPE.key
  private val incrementalFormatKey = DataSourceReadOptions.INCREMENTAL_FORMAT.key
  private val startCommitKey = DataSourceReadOptions.START_COMMIT.key
  private val schemaOnReadKey = "hoodie.schema.on.read.enable"

  private def explainPlan(sqlText: String): String =
    spark.sql(s"EXPLAIN $sqlText").collect().map(_.getString(0)).mkString("\n")

  private def containsBatchScan(plan: String): Boolean = plan.contains("BatchScan")

  private def containsFileScan(plan: String): Boolean = plan.contains("FileScan")

  private def createCowTable(tableName: String, tablePath: String): Unit = {
    spark.sql(
      s"""CREATE TABLE $tableName (
         |  id INT,
         |  name STRING,
         |  amount DOUBLE,
         |  ts LONG
         |) USING hudi
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = 'id',
         |  preCombineField = 'ts'
         |)
         |LOCATION '$tablePath'
         """.stripMargin)
  }

  test("Test SQL read stays on V1 when use.v2 is off") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createCowTable(tableName, s"${tmp.getCanonicalPath}/$tableName")
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10.0, 1000), (2, 'a2', 20.0, 2000)")

      // Default (flag off): the existing V1 path must be taken.
      val plan = explainPlan(s"SELECT * FROM $tableName")
      assert(!containsBatchScan(plan), s"With use.v2 off the SQL read must not use BatchScan, got:\n$plan")
      assert(containsFileScan(plan), s"With use.v2 off the SQL read should use a V1 FileScan, got:\n$plan")
      checkAnswer(s"SELECT id, name, amount FROM $tableName ORDER BY id")(
        Seq(1, "a1", 10.0),
        Seq(2, "a2", 20.0)
      )
    }
  }

  test("Test SQL read of supported COW snapshot opts into DSv2 with use.v2 on") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createCowTable(tableName, s"${tmp.getCanonicalPath}/$tableName")
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10.0, 1000)")

      withSQLConf(useV2Key -> "true") {
        spark.catalog.refreshTable(tableName)
        val plan = explainPlan(s"SELECT * FROM $tableName")
        assert(containsBatchScan(plan), s"COW snapshot with use.v2=true should use BatchScan, got:\n$plan")
      }

      // Back to V1 once the flag is unset; semantics unchanged.
      spark.catalog.refreshTable(tableName)
      val v1Plan = explainPlan(s"SELECT * FROM $tableName")
      assert(!containsBatchScan(v1Plan), s"After unsetting use.v2 the read must return to V1, got:\n$v1Plan")
      checkAnswer(s"SELECT id, name FROM $tableName")(Seq(1, "a1"))
    }
  }

  test("Test use.v2 from TBLPROPERTIES opts in without a session conf") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""CREATE TABLE $tableName (
           |  id INT,
           |  name STRING,
           |  ts LONG
           |) USING hudi
           |TBLPROPERTIES (
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'ts',
           |  '$useV2Key' = 'true'
           |)
           |LOCATION '$tablePath'
           """.stripMargin)
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 1000)")

      // Read options are resolved from catalog properties before gating, so the
      // table-level opt-in must be honored without any SET.
      val plan = explainPlan(s"SELECT * FROM $tableName")
      assert(containsBatchScan(plan),
        s"use.v2=true in TBLPROPERTIES should route the SQL read to BatchScan, got:\n$plan")
    }
  }

  test("Test schema-on-read takes precedence over use.v2") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createCowTable(tableName, s"${tmp.getCanonicalPath}/$tableName")
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10.0, 1000)")

      withSQLConf(schemaOnReadKey -> "true", useV2Key -> "true") {
        spark.catalog.refreshTable(tableName)
        // Schema evolution must keep the HoodieInternalV2Table path (V1 read fallback),
        // even with the DSv2 flag on.
        val plan = explainPlan(s"SELECT * FROM $tableName")
        assert(!containsBatchScan(plan),
          s"schema-on-read must take precedence over use.v2 and stay off BatchScan, got:\n$plan")
        checkAnswer(s"SELECT id, name FROM $tableName")(Seq(1, "a1"))
      }
    }
  }

  test("Test schema-on-read from TBLPROPERTIES takes precedence over use.v2") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      // schema-on-read enabled at table level only — no session conf. The V1 read path
      // honors it from the resolved read options, so the gate must too.
      spark.sql(
        s"""CREATE TABLE $tableName (
           |  id INT,
           |  name STRING,
           |  ts LONG
           |) USING hudi
           |TBLPROPERTIES (
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'ts',
           |  '$schemaOnReadKey' = 'true'
           |)
           |LOCATION '$tablePath'
           """.stripMargin)
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 1000)")

      withSQLConf(useV2Key -> "true") {
        spark.catalog.refreshTable(tableName)
        val plan = explainPlan(s"SELECT * FROM $tableName")
        assert(!containsBatchScan(plan),
          s"schema-on-read from TBLPROPERTIES must keep the read off BatchScan, got:\n$plan")
        checkAnswer(s"SELECT id, name FROM $tableName")(Seq(1, "a1"))
      }
    }
  }

  test("Test UPDATE and DELETE keep routing through Hudi commands with use.v2 on") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createCowTable(tableName, s"${tmp.getCanonicalPath}/$tableName")
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10.0, 1000), (2, 'a2', 20.0, 1000), (3, 'a3', 30.0, 1000)")

      withSQLConf(useV2Key -> "true") {
        spark.catalog.refreshTable(tableName)
        // The table resolves to HoodieSparkV2Table; the adapter must still resolve it as
        // a Hudi table and the DML targets must be converted back to V1 so the commands'
        // input scans see the data.
        spark.sql(s"UPDATE $tableName SET name = 'a1_new', amount = 15.0, ts = 2000 WHERE id = 1")
        spark.sql(s"DELETE FROM $tableName WHERE id = 2")
      }

      spark.catalog.refreshTable(tableName)
      checkAnswer(s"SELECT id, name, amount FROM $tableName ORDER BY id")(
        Seq(1, "a1_new", 15.0),
        Seq(3, "a3", 30.0)
      )
    }
  }

  test("Test MERGE INTO keeps routing through Hudi command with use.v2 on") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createCowTable(tableName, s"${tmp.getCanonicalPath}/$tableName")
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10.0, 1000), (2, 'a2', 20.0, 1000)")

      withSQLConf(useV2Key -> "true") {
        spark.catalog.refreshTable(tableName)
        spark.sql(
          s"""MERGE INTO $tableName AS t
             |USING (
             |  SELECT 1 AS id, 'a1_new' AS name, 12.0 AS amount, CAST(2000 AS BIGINT) AS ts
             |  UNION ALL
             |  SELECT 4 AS id, 'a4' AS name, 40.0 AS amount, CAST(2000 AS BIGINT) AS ts
             |) AS s
             |ON t.id = s.id
             |WHEN MATCHED THEN UPDATE SET *
             |WHEN NOT MATCHED THEN INSERT *
             """.stripMargin)
      }

      spark.catalog.refreshTable(tableName)
      checkAnswer(s"SELECT id, name, amount FROM $tableName ORDER BY id")(
        Seq(1, "a1_new", 12.0),
        Seq(2, "a2", 20.0),
        Seq(4, "a4", 40.0)
      )
    }
  }

  test("Test MOR snapshot read silently falls back to V1") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
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
           |  preCombineField = 'ts'
           |)
           |LOCATION '$tablePath'
           """.stripMargin)
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10.0, 1000), (2, 'a2', 20.0, 1000)")
      // UPDATE on MOR writes log files; a DSv2 path that dropped them would read stale values.
      spark.sql(s"UPDATE $tableName SET name = 'a1_new', amount = 15.0, ts = 2000 WHERE id = 1")

      withSQLConf(useV2Key -> "true") {
        spark.catalog.refreshTable(tableName)
        val plan = explainPlan(s"SELECT * FROM $tableName")
        assert(!containsBatchScan(plan) && containsFileScan(plan),
          s"MOR snapshot must silently fall back to a V1 FileScan, got:\n$plan")
        // The fallback must include the log-file updates.
        checkAnswer(s"SELECT id, name, amount FROM $tableName ORDER BY id")(
          Seq(1, "a1_new", 15.0),
          Seq(2, "a2", 20.0)
        )
      }
    }
  }

  test("Test MOR read_optimized via SQL conf uses DSv2") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""CREATE TABLE $tableName (
           |  id INT,
           |  name STRING,
           |  ts LONG
           |) USING hudi
           |TBLPROPERTIES (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           |)
           |LOCATION '$tablePath'
           """.stripMargin)
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 1000)")

      // The gate must consume resolved options: read_optimized arriving via SQL conf
      // flips the same MOR table from unsupported (snapshot) to supported.
      withSQLConf(useV2Key -> "true",
        queryTypeKey -> DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL) {
        spark.catalog.refreshTable(tableName)
        val plan = explainPlan(s"SELECT * FROM $tableName")
        assert(containsBatchScan(plan),
          s"MOR read_optimized via SQL conf should route to BatchScan, got:\n$plan")
      }
    }
  }

  test("Test incremental query via SQL conf silently falls back to V1") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      createCowTable(tableName, tablePath)
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10.0, 1000)")
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(tablePath)
        .setConf(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()))
        .build()
      val firstCommitCompletionTime = metaClient.getActiveTimeline
        .filterCompletedInstants().lastInstant().get().getCompletionTime
      spark.sql(s"INSERT INTO $tableName VALUES (2, 'a2', 20.0, 2000)")

      withSQLConf(useV2Key -> "true",
        queryTypeKey -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
        startCommitKey -> firstCommitCompletionTime) {
        spark.catalog.refreshTable(tableName)
        val plan = explainPlan(s"SELECT * FROM $tableName")
        assert(!containsBatchScan(plan),
          s"Incremental query via SQL conf must silently fall back to V1, got:\n$plan")
        // V1 incremental semantics must be preserved: the first commit's row must not
        // appear. The exact count of later rows is left to the V1 incremental relation.
        val ids = spark.sql(s"SELECT id FROM $tableName").collect().map(_.getInt(0)).toSet
        assert(!ids.contains(1),
          s"Incremental read must not include the first commit's row (id=1); got ids=$ids")
      }
    }
  }

  test("Test CDC query via SQL conf silently falls back to V1") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""CREATE TABLE $tableName (
           |  id INT,
           |  name STRING,
           |  ts LONG
           |) USING hudi
           |TBLPROPERTIES (
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'ts',
           |  'hoodie.table.cdc.enabled' = 'true'
           |)
           |LOCATION '$tablePath'
           """.stripMargin)
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 1000), (2, 'a2', 1000)")
      spark.sql(s"UPDATE $tableName SET name = 'a1_new', ts = 2000 WHERE id = 1")

      withSQLConf(useV2Key -> "true",
        queryTypeKey -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
        incrementalFormatKey -> DataSourceReadOptions.INCREMENTAL_FORMAT_CDC_VAL,
        startCommitKey -> "000") {
        spark.catalog.refreshTable(tableName)
        val plan = explainPlan(s"SELECT * FROM $tableName")
        assert(!containsBatchScan(plan),
          s"CDC query via SQL conf must silently fall back to V1, got:\n$plan")
        // The V1 CDC relation exposes the change-data schema; reaching it proves the
        // fallback resolved with CDC semantics rather than a plain snapshot.
        val columns = spark.sql(s"SELECT * FROM $tableName").columns.toSet
        assert(columns.contains("op"),
          s"CDC fallback should expose the CDC schema with an 'op' column; got columns=$columns")
      }
    }
  }

  test("Test bootstrap table read silently falls back to V1") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val sourcePath = s"${tmp.getCanonicalPath}/${tableName}_src"
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      val _spark = spark
      import _spark.implicits._

      Seq((1, "a1"), (2, "a2"), (3, "a3"))
        .toDF("id", "name")
        .write.parquet(s"$sourcePath/datestr=2021")

      withSQLConf(
        "hoodie.bootstrap.parallelism" -> "4",
        "hoodie.metadata.index.column.stats.enable" -> "false") {
        checkAnswer(
          s"""CALL run_bootstrap(
             |table => '$tableName',
             |base_path => '$tablePath',
             |table_type => 'COPY_ON_WRITE',
             |bootstrap_path => '$sourcePath',
             |rowKey_field => 'id',
             |partition_path_field => 'datestr',
             |bootstrap_overwrite => true)""".stripMargin)(Seq(0))
      }
      spark.sql(s"CREATE TABLE $tableName USING hudi LOCATION '$tablePath' TBLPROPERTIES (primaryKey = 'id')")

      withSQLConf(useV2Key -> "true") {
        spark.catalog.refreshTable(tableName)
        val plan = explainPlan(s"SELECT * FROM $tableName")
        assert(!containsBatchScan(plan),
          s"Bootstrap table must silently fall back to V1, got:\n$plan")
        checkAnswer(s"SELECT id, name FROM $tableName ORDER BY id")(
          Seq(1, "a1"),
          Seq(2, "a2"),
          Seq(3, "a3")
        )
      }
    }
  }

  test("Test Lance base file format read silently falls back to V1") {
    assume(!"true".equalsIgnoreCase(System.getProperty("lance.skip.tests")),
      "Lance tests are disabled via -Dlance.skip.tests=true")
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""CREATE TABLE $tableName (
           |  id INT,
           |  name STRING,
           |  ts LONG
           |) USING hudi
           |TBLPROPERTIES (
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'ts',
           |  hoodie.table.base.file.format = 'LANCE',
           |  hoodie.datasource.write.record.merger.impls = '${classOf[DefaultSparkRecordMerger].getName}'
           |)
           |LOCATION '$tablePath'
           """.stripMargin)
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 1000), (2, 'a2', 2000)")

      withSQLConf(useV2Key -> "true") {
        spark.catalog.refreshTable(tableName)
        val plan = explainPlan(s"SELECT * FROM $tableName")
        assert(!containsBatchScan(plan),
          s"Non-Parquet (Lance) base format must silently fall back to V1, got:\n$plan")
        checkAnswer(s"SELECT id, name FROM $tableName ORDER BY id")(
          Seq(1, "a1"),
          Seq(2, "a2")
        )
      }
    }
  }

  test("Test partition DDL still resolves when the table routes to DSv2") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""CREATE TABLE $tableName (
           |  id INT,
           |  name STRING,
           |  ts LONG,
           |  country STRING
           |) USING hudi
           |TBLPROPERTIES (
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           |)
           |PARTITIONED BY (country)
           |LOCATION '$tablePath'
           """.stripMargin)
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 1000, 'US'), (2, 'a2', 2000, 'UK')")

      withSQLConf(useV2Key -> "true") {
        spark.catalog.refreshTable(tableName)
        // loadTable now returns HoodieSparkV2Table; the HoodieV1OrV2Table extractor must
        // keep routing partition DDL to the Hudi commands.
        val partitions = spark.sql(s"SHOW PARTITIONS $tableName").collect().map(_.getString(0)).sorted
        assertResult(Seq("country=UK", "country=US"))(partitions.toSeq)

        spark.sql(s"ALTER TABLE $tableName DROP PARTITION (country='UK')")
        val remaining = spark.sql(s"SHOW PARTITIONS $tableName").collect().map(_.getString(0))
        assertResult(Seq("country=US"))(remaining.toSeq)

        spark.sql(s"TRUNCATE TABLE $tableName")
      }
      spark.catalog.refreshTable(tableName)
      assertResult(0)(spark.sql(s"SELECT * FROM $tableName").count())
    }
  }
}
