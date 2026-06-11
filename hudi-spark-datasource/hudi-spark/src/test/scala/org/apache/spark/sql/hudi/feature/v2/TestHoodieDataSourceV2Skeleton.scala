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
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.exception.HoodieException

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.v2.{HoodieDataSourceV2, HoodieSparkV2Table}

import java.io.File

import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * Smoke tests for `hudi_v2` coexistence with DSv1: provider lookup through the SPI,
 * `BatchScan` planning, schema/properties/capabilities of the path-based
 * [[HoodieSparkV2Table]], and the V1 write fallback of `df.write.format("hudi_v2")`.
 * `format("hudi")` must remain bit-for-bit unaffected.
 */
class TestHoodieDataSourceV2Skeleton extends HoodieSparkSqlTestBase {

  private def writeCowTable(path: String, tableName: String, mode: SaveMode = SaveMode.Overwrite,
                            extraOptions: Map[String, String] = Map.empty): Unit = {
    val _spark = spark
    import _spark.implicits._
    Seq((1, "a1", 10.0, 1000L), (2, "a2", 20.0, 2000L), (3, "a3", 30.0, 3000L))
      .toDF("id", "name", "price", "ts")
      .write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .options(extraOptions)
      .mode(mode)
      .save(path)
  }

  private def planOf(df: DataFrame): String = df.queryExecution.executedPlan.toString()

  test("Test hudi_v2 provider lookup through DataSourceRegister") {
    val providerClass = DataSource.lookupDataSource("hudi_v2", spark.sessionState.conf)
    assertResult(classOf[HoodieDataSourceV2])(providerClass)
    assertResult("hudi_v2")(new HoodieDataSourceV2().shortName())
  }

  test("Test hudi_v2 read plans a BatchScan and leaves format hudi untouched") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      writeCowTable(path, tableName)

      val v2Df = spark.read.format("hudi_v2").load(path)
      val v2Plan = planOf(v2Df)
      assert(v2Plan.contains("BatchScan"), s"hudi_v2 read should plan a BatchScan, got:\n$v2Plan")
      checkAnswer(v2Df.select("id", "name", "price", "ts").orderBy("id").collect())(
        Seq(1, "a1", 10.0, 1000L),
        Seq(2, "a2", 20.0, 2000L),
        Seq(3, "a3", 30.0, 3000L)
      )

      val v1Df = spark.read.format("hudi").load(path)
      val v1Plan = planOf(v1Df)
      assert(!v1Plan.contains("BatchScan"), s"format('hudi') must stay on the V1 path, got:\n$v1Plan")
      assert(v1Plan.contains("FileScan"), s"format('hudi') should plan a FileScan, got:\n$v1Plan")
      assertResult(3)(v1Df.count())
    }
  }

  test("Test path-based hudi_v2 table exposes schema with meta fields and read-only capabilities") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      writeCowTable(path, tableName)

      val table = HoodieSparkV2Table(spark, path)
      assertResult(tableName)(table.name())

      // populate.meta.fields defaults to true, so the V2 schema must lead with the meta
      // columns followed by the data columns, mirroring what DSv1 exposes.
      val metaFields = HoodieRecord.HOODIE_META_COLUMNS.asScala
      assertResult(metaFields ++ Seq("id", "name", "price", "ts"))(table.schema().fieldNames.toSeq)

      // Without a catalog table there are no table properties and no write capabilities:
      // path writes go through the CreatableRelationProvider fallback instead.
      assert(table.properties().isEmpty)
      assertResult(1)(table.capabilities().size())
      assert(table.capabilities().contains(TableCapability.BATCH_READ))
      assert(table.partitioning().isEmpty)

      // The DataFrame read schema must match the table schema.
      assertResult(table.schema())(spark.read.format("hudi_v2").load(path).schema)
    }
  }

  test("Test hudi_v2 load of a partition path walks up to the base path preserving the query path") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      val _spark = spark
      import _spark.implicits._
      Seq((1, "a1", 10.0, 1000L, "US"), (2, "a2", 20.0, 2000L, "UK"))
        .toDF("id", "name", "price", "ts", "country")
        .write.format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .option("hoodie.datasource.write.partitionpath.field", "country")
        .mode(SaveMode.Overwrite)
        .save(path)

      val partitionPath = new File(path, "US").getCanonicalPath

      // The meta client must be built at the table base path (where .hoodie lives), while
      // the user-supplied partition path is kept as the query path on the table.
      val table = HoodieSparkV2Table(spark, partitionPath)
      assertResult(tableName)(table.name())
      assertResult(partitionPath)(table.path)
      assert(table.schema().fieldNames.contains("country"))

      // The scan must stay scoped to the partition the user pointed at.
      val df = spark.read.format("hudi_v2").load(partitionPath)
      assert(planOf(df).contains("BatchScan"))
      checkAnswer(df.select("id", "name", "country").collect())(
        Seq(1, "a1", "US")
      )
    }
  }

  test("Test hudi_v2 path write falls back to the V1 writer") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      val _spark = spark
      import _spark.implicits._

      Seq((1, "a1", 10.0, 1000L), (2, "a2", 20.0, 2000L), (3, "a3", 30.0, 3000L))
        .toDF("id", "name", "price", "ts")
        .write.format("hudi_v2")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .mode(SaveMode.Overwrite)
        .save(path)

      assert(new File(path, ".hoodie").exists(), "hudi_v2 write should have initialized a Hudi table")

      // The data must be readable through the untouched V1 path.
      checkAnswer(spark.read.format("hudi").load(path)
        .select("id", "name", "price", "ts").orderBy("id").collect())(
        Seq(1, "a1", 10.0, 1000L),
        Seq(2, "a2", 20.0, 2000L),
        Seq(3, "a3", 30.0, 3000L)
      )

      // Subsequent appends flow through the same fallback.
      Seq((4, "a4", 40.0, 4000L))
        .toDF("id", "name", "price", "ts")
        .write.format("hudi_v2")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .mode(SaveMode.Append)
        .save(path)

      assertResult(4)(spark.read.format("hudi").load(path).count())
    }
  }

  test("Test hudi_v2 read of unsupported configurations is rejected by the gate") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      writeCowTable(path, tableName,
        extraOptions = Map("hoodie.datasource.write.table.type" -> "MERGE_ON_READ"))

      // MOR snapshot needs log merging, which the DSv2 scan does not implement yet; the
      // DataFrame path must fail explicitly instead of silently returning base-file data.
      intercept[HoodieException] {
        spark.read.format("hudi_v2").load(path).collect()
      }

      // An explicitly requested read-optimized query is base-file-only and passes the gate.
      val roDf = spark.read.format("hudi_v2")
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
        .load(path)
      assert(planOf(roDf).contains("BatchScan"))
      assertResult(3)(roDf.count())
    }
  }
}
