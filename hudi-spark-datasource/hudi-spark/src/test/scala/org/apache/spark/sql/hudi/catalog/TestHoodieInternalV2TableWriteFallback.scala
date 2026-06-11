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

package org.apache.spark.sql.hudi.catalog

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

import java.io.File

/**
 * Tests for the V1 write fallback of [[HoodieInternalV2Table]] ([[HoodieV1WriteBuilder]]).
 *
 * `hoodie.schema.on.read.enable=true` makes [[HoodieCatalog.loadTable]] return
 * [[HoodieInternalV2Table]] instead of the plain `V1Table` wrapper, so SQL statements
 * resolve against the V2 table while writes fall back to the V1 path:
 * - `INSERT INTO` / `INSERT OVERWRITE` statements are converted back to V1 relations by the
 *   per-Spark-version `DataSourceV2ToV1Fallback` rule;
 * - DataFrameWriterV2 (`df.writeTo(...)`) plans (`AppendData`, `OverwriteByExpression`) keep the
 *   V2 table and exercise [[HoodieV1WriteBuilder]] through the `V1_BATCH_WRITE` capability.
 */
class TestHoodieInternalV2TableWriteFallback extends HoodieSparkSqlTestBase {

  private val schemaOnReadConf = "hoodie.schema.on.read.enable" -> "true"

  private def createNonPartitionedTable(tableName: String, tablePath: String): Unit = {
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         |) using hudi
         | tblproperties (
         |  type = 'cow',
         |  primaryKey = 'id',
         |  preCombineField = 'ts'
         | )
         | location '$tablePath'
       """.stripMargin)
  }

  private def createPartitionedTable(tableName: String, tablePath: String): Unit = {
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  country string
         |) using hudi
         | tblproperties (
         |  type = 'cow',
         |  primaryKey = 'id'
         | )
         | partitioned by (country)
         | location '$tablePath'
       """.stripMargin)
  }

  test("Test insert into V2 table") {
    withSQLConf(schemaOnReadConf) {
      withTempDir { tmp =>
        val tableName = generateTableName
        createNonPartitionedTable(tableName, s"${tmp.getCanonicalPath}/$tableName")

        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000), (2, 'a2', 20.0, 2000)")
        spark.sql(s"insert into $tableName values (3, 'a3', 30.0, 3000)")

        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 20.0, 2000),
          Seq(3, "a3", 30.0, 3000)
        )
      }
    }
  }

  test("Test insert overwrite table on V2 table") {
    withSQLConf(schemaOnReadConf) {
      withTempDir { tmp =>
        val tableName = generateTableName
        createNonPartitionedTable(tableName, s"${tmp.getCanonicalPath}/$tableName")

        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000), (2, 'a2', 20.0, 2000)")
        spark.sql(s"insert overwrite table $tableName values (3, 'a3', 30.0, 3000)")

        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(3, "a3", 30.0, 3000)
        )
      }
    }
  }

  test("Test insert overwrite partition on V2 table") {
    withSQLConf(schemaOnReadConf) {
      withTempDir { tmp =>
        val tableName = generateTableName
        createPartitionedTable(tableName, s"${tmp.getCanonicalPath}/$tableName")

        spark.sql(s"insert into $tableName partition (country='US') values (1, 'a1', 10.0), (2, 'a2', 20.0)")
        spark.sql(s"insert into $tableName partition (country='UK') values (3, 'a3', 30.0)")

        // Overwrite only the US partition; the UK partition must be preserved.
        spark.sql(s"insert overwrite table $tableName partition (country='US') values (4, 'a4', 40.0)")

        checkAnswer(s"select id, name, price, country from $tableName order by id")(
          Seq(3, "a3", 30.0, "UK"),
          Seq(4, "a4", 40.0, "US")
        )
      }
    }
  }

  test("Test writeTo append aligns VALUES-clause column names and types") {
    withSQLConf(schemaOnReadConf) {
      withTempDir { tmp =>
        val tableName = generateTableName
        createNonPartitionedTable(tableName, s"${tmp.getCanonicalPath}/$tableName")

        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000)")

        // A VALUES relation carries generic column names (col1, col2, ...) and uncast literal
        // types (DECIMAL for the double column, INT for the long column). Since the table
        // accepts any schema, Spark hands the DataFrame to the V1 write fallback as-is, which
        // must rename and cast the columns to the table's user schema.
        spark.sql("values (2, 'a2', 20.0, 2000)").writeTo(tableName).append()

        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 20.0, 2000)
        )
      }
    }
  }

  test("Test writeTo append aligns reordered named columns by name") {
    withSQLConf(schemaOnReadConf) {
      withTempDir { tmp =>
        val tableName = generateTableName
        createNonPartitionedTable(tableName, s"${tmp.getCanonicalPath}/$tableName")

        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000)")

        // DataFrameWriterV2 writes are by-name: a source carrying the table's column names
        // in a different order must be realigned by name, not zipped positionally.
        spark.sql("select 2000L as ts, 'a2' as name, 20.0 as price, 2 as id")
          .writeTo(tableName).append()

        // Column-name matching must be case-insensitive (Spark's default resolver).
        spark.sql("select 3000L as TS, 'a3' as NAME, 30.0 as PRICE, 3 as ID")
          .writeTo(tableName).append()

        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 20.0, 2000),
          Seq(3, "a3", 30.0, 3000)
        )
      }
    }
  }

  test("Test writeTo append with same-arity mismatched column names is rejected") {
    withSQLConf(schemaOnReadConf) {
      withTempDir { tmp =>
        val tableName = generateTableName
        createNonPartitionedTable(tableName, s"${tmp.getCanonicalPath}/$tableName")

        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000)")

        // A same-width source with a misspelled column ('amount' instead of 'price') must
        // not be silently remapped by position; only generic VALUES columns (col1, ...)
        // qualify for positional alignment.
        checkExceptionContain(() => {
          spark.sql("select 2 as id, 'a2' as name, 20.0 as amount, 2000L as ts")
            .writeTo(tableName).append()
        })("Cannot align the incoming write schema")

        // The rejected write must not have changed the table.
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000)
        )
      }
    }
  }

  test("Test writeTo append cannot retarget the write with a path option") {
    withSQLConf(schemaOnReadConf) {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        createNonPartitionedTable(tableName, tablePath)

        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000)")

        // Catalog-owned settings must stay authoritative over per-write options: a
        // user-supplied 'path' must not redirect the write away from the target table.
        val otherPath = s"${tmp.getCanonicalPath}/elsewhere"
        spark.sql("select 2 as id, 'a2' as name, 20.0 as price, 2000L as ts")
          .writeTo(tableName).option("path", otherPath).append()

        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 20.0, 2000)
        )
        assertResult(false)(new File(otherPath).exists())
      }
    }
  }

  test("Test writeTo overwrite with always-true condition truncates V2 table") {
    withSQLConf(schemaOnReadConf) {
      withTempDir { tmp =>
        val tableName = generateTableName
        createNonPartitionedTable(tableName, s"${tmp.getCanonicalPath}/$tableName")

        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000), (2, 'a2', 20.0, 2000)")

        // An always-true overwrite condition maps to SupportsTruncate.truncate(), i.e.
        // an insert-overwrite-table write.
        spark.sql("values (3, 'a3', 30.0, 3000)").writeTo(tableName).overwrite(lit(true))

        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(3, "a3", 30.0, 3000)
        )
      }
    }
  }

  test("Test writeTo overwrite by arbitrary filter is rejected on V2 table") {
    withSQLConf(schemaOnReadConf) {
      withTempDir { tmp =>
        val tableName = generateTableName
        createNonPartitionedTable(tableName, s"${tmp.getCanonicalPath}/$tableName")

        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000), (2, 'a2', 20.0, 2000)")

        // OVERWRITE_BY_FILTER is not advertised because the V1 write fallback cannot honor an
        // arbitrary filter expression — it only supports full-table or full-partition
        // overwrites. Spark must reject the write at analysis time instead of silently
        // rewriting rows outside the filter.
        intercept[AnalysisException] {
          spark.sql("values (1, 'a1_new', 11.0, 1100)").writeTo(tableName)
            .overwrite(col("id") === lit(1))
        }

        // The rejected write must not have changed the table.
        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 20.0, 2000)
        )
      }
    }
  }
}
