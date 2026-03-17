// Hudi Benchmark — COW vs MOR, DSv1 vs DSv2

// ---------------------------------------------------------------------------
// Section 1: Configuration
// ---------------------------------------------------------------------------

require(sys.env.getOrElse("HUDI_BENCHMARK_DATA_PATH", "").nonEmpty,
  "HUDI_BENCHMARK_DATA_PATH environment variable must be set and non-empty")
require(sys.env.getOrElse("HUDI_BENCHMARK_RECORD_KEY", "").nonEmpty,
  "HUDI_BENCHMARK_RECORD_KEY environment variable must be set and non-empty")
require(sys.env.getOrElse("HUDI_BENCHMARK_PRECOMBINE_FIELD", "").nonEmpty,
  "HUDI_BENCHMARK_PRECOMBINE_FIELD environment variable must be set and non-empty")

val dataPath = sys.env("HUDI_BENCHMARK_DATA_PATH")
val recordKey = sys.env("HUDI_BENCHMARK_RECORD_KEY")
val precombineField = sys.env("HUDI_BENCHMARK_PRECOMBINE_FIELD")
val partitionField = sys.env.getOrElse("HUDI_BENCHMARK_PARTITION_FIELD", "")
val iterations = sys.env.getOrElse("HUDI_BENCHMARK_ITERATIONS", "1").toInt
val projectedColsEnv = sys.env.getOrElse("HUDI_BENCHMARK_PROJECTED_COLS", "")
val dsv2Enabled = sys.env.getOrElse("HUDI_BENCHMARK_DSV2_ENABLED", "true").toBoolean
val limitValue = sys.env.getOrElse("HUDI_BENCHMARK_LIMIT_VALUE", "1000").toInt
val filterColEnv = sys.env.getOrElse("HUDI_BENCHMARK_FILTER_COL", "")

val partitionClause = if (partitionField.nonEmpty) s"partitionedBy '$partitionField'" else ""
val partitionTblProp = if (partitionField.nonEmpty) s"'hoodie.datasource.write.partitionpath.field' = '$partitionField'," else ""

// Table name constants
val cowBulkInsert = "cow_bulk_insert"
val morBulkInsert = "mor_bulk_insert"
val cowInsert = "cow_insert"
val morInsert = "mor_insert"

// ---------------------------------------------------------------------------
// Section 2: Logging Setup
// ---------------------------------------------------------------------------

import java.io.{File, FileOutputStream, OutputStream, PrintStream}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class TeeOutputStream(primary: OutputStream, secondary: OutputStream) extends OutputStream {
  override def write(b: Int): Unit = { primary.write(b); secondary.write(b) }
  override def write(b: Array[Byte]): Unit = { primary.write(b); secondary.write(b) }
  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    primary.write(b, off, len); secondary.write(b, off, len)
  }
  override def flush(): Unit = { primary.flush(); secondary.flush() }
  override def close(): Unit = { secondary.close() }
}

val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
val benchmarkDb = s"hudi_benchmark_$timestamp"
val logFileName = s"hudi_benchmark_${timestamp}.log"
val logFile = new FileOutputStream(new File(logFileName))
val origOut = System.out
val teeOut = new PrintStream(new TeeOutputStream(origOut, logFile), true)
System.setOut(teeOut)
Console.setOut(teeOut)

println(s"Benchmark log file: $logFileName")
println(s"Configuration:")
println(s"  Data path:        $dataPath")
println(s"  Record key:       $recordKey")
println(s"  Precombine field: $precombineField")
println(s"  Partition field:  ${if (partitionField.nonEmpty) partitionField else "(none)"}")
println(s"  Iterations:       $iterations")
println(s"  DSv2 enabled:     $dsv2Enabled")
println(s"  Limit value:      $limitValue")
println(s"  Filter column:    ${if (filterColEnv.nonEmpty) filterColEnv else "(auto-detect)"}")

// ---------------------------------------------------------------------------
// Section 3: Helper Functions
// ---------------------------------------------------------------------------

def timed(label: String)(block: => Unit): Double = {
  System.gc()
  val start = System.nanoTime()
  block
  val elapsed = (System.nanoTime() - start) / 1e9
  println(f"  $label: $elapsed%.1fs")
  elapsed
}

def stats(times: Seq[Double]): (Double, Double, Double) = {
  (times.min, times.max, times.sum / times.size)
}

def printStats(label: String, times: Seq[Double]): Unit = {
  val (mn, mx, avg) = stats(times)
  val timesStr = times.map(t => f"$t%.1fs").mkString(", ")
  println(f"$label: $timesStr  (min: $mn%.1fs, max: $mx%.1fs, avg: $avg%.1fs)")
}

def getTablePath(tableName: String): String = {
  import org.apache.spark.sql.catalyst.TableIdentifier
  spark.sessionState.catalog.getTableMetadata(
    TableIdentifier(tableName, Some(benchmarkDb))
  ).location.toString
}

def getLastCompletionTime(tablePath: String): String = {
  import org.apache.hudi.common.table.HoodieTableMetaClient
  import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
  val hadoopConf = spark.sparkContext.hadoopConfiguration
  val metaClient = HoodieTableMetaClient.builder()
    .setConf(new HadoopStorageConfiguration(hadoopConf))
    .setBasePath(tablePath)
    .build()
  metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().getCompletionTime
}

// ---------------------------------------------------------------------------
// Section 4: Source Data Loading
// ---------------------------------------------------------------------------

import org.apache.spark.storage.StorageLevel

spark.sql(s"CREATE DATABASE IF NOT EXISTS $benchmarkDb")
spark.sql(s"USE $benchmarkDb")
println(s"\nUsing database: $benchmarkDb")

println("\n=== Loading and caching source data ===")
timed("Load and cache source data") {
  val df = spark.read.parquet(dataPath)
    .persist(StorageLevel.MEMORY_AND_DISK_SER)
  val rowCount = df.count() // materialize cache
  df.createOrReplaceTempView("source_data")
  println(s"  Cached $rowCount rows")
}

// Auto-detect projected columns if not specified
val projectedCols = if (projectedColsEnv.nonEmpty) {
  projectedColsEnv
} else {
  val allCols = spark.sql("SELECT * FROM source_data LIMIT 0").columns
  val keyFields = Set(recordKey, precombineField, partitionField).filter(_.nonEmpty)
  val nonKeyCols = allCols.filterNot(keyFields.contains)
  val selected = nonKeyCols.take(6)
  println(s"  Auto-detected projected columns: ${selected.mkString(", ")}")
  selected.mkString(",")
}

// Report cache statistics
println("\n--- Cache Statistics ---")
spark.sparkContext.getRDDStorageInfo.foreach { info =>
  println(s"  RDD ${info.id} (${info.name}): " +
    s"${info.numCachedPartitions} partitions, " +
    s"memory: ${info.memSize / (1024 * 1024)}MB, " +
    s"disk: ${info.diskSize / (1024 * 1024)}MB")
}

// Print schema summary
println("\n--- Schema Summary ---")
val schemaFields = spark.sql("SELECT * FROM source_data LIMIT 0").schema.fields
println(s"  Total columns: ${schemaFields.length}")
println(s"  First 10: ${schemaFields.take(10).map(f => s"${f.name}:${f.dataType.simpleString}").mkString(", ")}")

// Auto-detect filter column and value for filter pushdown benchmark
val filterCol = if (filterColEnv.nonEmpty) filterColEnv else projectedCols.split(",").head.trim
val (filterExpr, filterIsNumeric) = {
  try {
    val midRow = spark.sql(
      s"SELECT percentile_approx(CAST($filterCol AS DOUBLE), 0.5) FROM source_data"
    ).collect()
    val filterValue = midRow(0).get(0)
    (s"$filterCol >= $filterValue", true)
  } catch {
    case _: Exception =>
      (s"$filterCol IS NOT NULL", false)
  }
}
println(s"\n--- Filter Configuration ---")
println(s"  Filter column: $filterCol")
println(s"  Filter expression: $filterExpr")
println(s"  Numeric filter: $filterIsNumeric")

// Track tables created during benchmark for cleanup
val createdTables = scala.collection.mutable.ListBuffer[String]()

// ---------------------------------------------------------------------------
// Section 5: Benchmark Suite
// ---------------------------------------------------------------------------

// Collectors for final summary
var cowBulkInsertTimes: Seq[Double] = Seq.empty
var morBulkInsertTimes: Seq[Double] = Seq.empty
var cowInsertTimes: Seq[Double] = Seq.empty
var morInsertTimes: Seq[Double] = Seq.empty
var readFullCowDsv1Times: Seq[Double] = Seq.empty
var readFullMorDsv1Times: Seq[Double] = Seq.empty
var readFullCowDsv2Times: Seq[Double] = Seq.empty
var readFullMorDsv2Times: Seq[Double] = Seq.empty
var readProjCowDsv1Times: Seq[Double] = Seq.empty
var readProjMorDsv1Times: Seq[Double] = Seq.empty
var readProjCowDsv2Times: Seq[Double] = Seq.empty
var readProjMorDsv2Times: Seq[Double] = Seq.empty
var compactionTimes: Seq[Double] = Seq.empty
var clusteringTimes: Seq[Double] = Seq.empty
var readFilterCowDsv1Times: Seq[Double] = Seq.empty
var readFilterMorDsv1Times: Seq[Double] = Seq.empty
var readFilterCowDsv2Times: Seq[Double] = Seq.empty
var readFilterMorDsv2Times: Seq[Double] = Seq.empty
var readLimitCowDsv1Times: Seq[Double] = Seq.empty
var readLimitMorDsv1Times: Seq[Double] = Seq.empty
var readLimitCowDsv2Times: Seq[Double] = Seq.empty
var readLimitMorDsv2Times: Seq[Double] = Seq.empty
var aggCountDsv1Times: Seq[Double] = Seq.empty
var aggCountDsv2Times: Seq[Double] = Seq.empty
var aggMinMaxDsv1Times: Seq[Double] = Seq.empty
var aggMinMaxDsv2Times: Seq[Double] = Seq.empty
var incrReadDsv1Times: Seq[Double] = Seq.empty
var incrReadDsv2Times: Seq[Double] = Seq.empty
var cdcReadDsv1Times: Seq[Double] = Seq.empty
var cdcReadDsv2Times: Seq[Double] = Seq.empty

try {
  // -------------------------------------------------------------------------
  // 5a: Write Benchmark — bulk_insert
  // -------------------------------------------------------------------------

  println("\n=== WRITE BENCHMARK (bulk_insert) ===")

  // Warmup
  println("--- Warmup ---")
  val warmupCowBulk = s"${cowBulkInsert}_warmup"
  spark.sql(s"DROP TABLE IF EXISTS $warmupCowBulk PURGE")
  spark.sql("SET hoodie.datasource.write.operation = bulk_insert")
  timed("Warmup COW bulk_insert") {
    spark.sql(
      s"""CREATE TABLE $warmupCowBulk USING hudi
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = '$recordKey',
         |  preCombineField = '$precombineField',
         |  $partitionTblProp
         |  'hoodie.compact.inline' = 'false',
         |  'hoodie.clustering.inline' = 'false',
         |  'hoodie.bulkinsert.shuffle.parallelism' = '200'
         |) $partitionClause AS SELECT * FROM source_data LIMIT 100000""".stripMargin)
  }
  spark.sql(s"DROP TABLE IF EXISTS $warmupCowBulk PURGE")

  val warmupMorBulk = s"${morBulkInsert}_warmup"
  spark.sql(s"DROP TABLE IF EXISTS $warmupMorBulk PURGE")
  timed("Warmup MOR bulk_insert") {
    spark.sql(
      s"""CREATE TABLE $warmupMorBulk USING hudi
         |TBLPROPERTIES (
         |  type = 'mor',
         |  primaryKey = '$recordKey',
         |  preCombineField = '$precombineField',
         |  $partitionTblProp
         |  'hoodie.compact.inline' = 'false',
         |  'hoodie.clustering.inline' = 'false',
         |  'hoodie.bulkinsert.shuffle.parallelism' = '200'
         |) $partitionClause AS SELECT * FROM source_data LIMIT 100000""".stripMargin)
  }
  spark.sql(s"DROP TABLE IF EXISTS $warmupMorBulk PURGE")

  println("\n--- Timed iterations ---")

  // COW bulk_insert
  spark.sql("SET hoodie.datasource.write.operation = bulk_insert")
  cowBulkInsertTimes = (1 to iterations).map { i =>
    val tableName = s"${cowBulkInsert}_$i"
    spark.sql(s"DROP TABLE IF EXISTS $tableName PURGE")
    createdTables += tableName
    timed(s"COW bulk_insert (iter $i)") {
      spark.sql(
        s"""CREATE TABLE $tableName USING hudi
           |TBLPROPERTIES (
           |  type = 'cow',
           |  primaryKey = '$recordKey',
           |  preCombineField = '$precombineField',
           |  $partitionTblProp
           |  'hoodie.compact.inline' = 'false',
           |  'hoodie.clustering.inline' = 'false',
           |  'hoodie.bulkinsert.shuffle.parallelism' = '200'
           |) $partitionClause AS SELECT * FROM source_data""".stripMargin)
    }
  }

  // MOR bulk_insert
  morBulkInsertTimes = (1 to iterations).map { i =>
    val tableName = s"${morBulkInsert}_$i"
    spark.sql(s"DROP TABLE IF EXISTS $tableName PURGE")
    createdTables += tableName
    timed(s"MOR bulk_insert (iter $i)") {
      spark.sql(
        s"""CREATE TABLE $tableName USING hudi
           |TBLPROPERTIES (
           |  type = 'mor',
           |  primaryKey = '$recordKey',
           |  preCombineField = '$precombineField',
           |  $partitionTblProp
           |  'hoodie.compact.inline' = 'false',
           |  'hoodie.clustering.inline' = 'false',
           |  'hoodie.bulkinsert.shuffle.parallelism' = '200'
           |) $partitionClause AS SELECT * FROM source_data""".stripMargin)
    }
  }

  println("\n--- Write bulk_insert Summary ---")
  printStats("COW bulk_insert", cowBulkInsertTimes)
  printStats("MOR bulk_insert", morBulkInsertTimes)

  // -------------------------------------------------------------------------
  // 5b: Write Benchmark — insert
  // -------------------------------------------------------------------------

  println("\n=== WRITE BENCHMARK (insert) ===")

  // Warmup
  println("--- Warmup ---")
  val warmupCowInsert = s"${cowInsert}_warmup"
  spark.sql(s"DROP TABLE IF EXISTS $warmupCowInsert PURGE")
  spark.sql("SET hoodie.datasource.write.operation = insert")
  timed("Warmup COW insert") {
    spark.sql(
      s"""CREATE TABLE $warmupCowInsert USING hudi
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = '$recordKey',
         |  preCombineField = '$precombineField',
         |  $partitionTblProp
         |  'hoodie.compact.inline' = 'false',
         |  'hoodie.clustering.inline' = 'false'
         |) $partitionClause AS SELECT * FROM source_data LIMIT 100000""".stripMargin)
  }
  spark.sql(s"DROP TABLE IF EXISTS $warmupCowInsert PURGE")

  val warmupMorInsert = s"${morInsert}_warmup"
  spark.sql(s"DROP TABLE IF EXISTS $warmupMorInsert PURGE")
  timed("Warmup MOR insert") {
    spark.sql(
      s"""CREATE TABLE $warmupMorInsert USING hudi
         |TBLPROPERTIES (
         |  type = 'mor',
         |  primaryKey = '$recordKey',
         |  preCombineField = '$precombineField',
         |  $partitionTblProp
         |  'hoodie.compact.inline' = 'false',
         |  'hoodie.clustering.inline' = 'false'
         |) $partitionClause AS SELECT * FROM source_data LIMIT 100000""".stripMargin)
  }
  spark.sql(s"DROP TABLE IF EXISTS $warmupMorInsert PURGE")

  println("\n--- Timed iterations ---")

  // COW insert
  spark.sql("SET hoodie.datasource.write.operation = insert")
  cowInsertTimes = (1 to iterations).map { i =>
    val tableName = s"${cowInsert}_$i"
    spark.sql(s"DROP TABLE IF EXISTS $tableName PURGE")
    createdTables += tableName
    timed(s"COW insert (iter $i)") {
      spark.sql(
        s"""CREATE TABLE $tableName USING hudi
           |TBLPROPERTIES (
           |  type = 'cow',
           |  primaryKey = '$recordKey',
           |  preCombineField = '$precombineField',
           |  $partitionTblProp
           |  'hoodie.compact.inline' = 'false',
           |  'hoodie.clustering.inline' = 'false'
           |) $partitionClause AS SELECT * FROM source_data""".stripMargin)
    }
  }

  // MOR insert
  morInsertTimes = (1 to iterations).map { i =>
    val tableName = s"${morInsert}_$i"
    spark.sql(s"DROP TABLE IF EXISTS $tableName PURGE")
    createdTables += tableName
    timed(s"MOR insert (iter $i)") {
      spark.sql(
        s"""CREATE TABLE $tableName USING hudi
           |TBLPROPERTIES (
           |  type = 'mor',
           |  primaryKey = '$recordKey',
           |  preCombineField = '$precombineField',
           |  $partitionTblProp
           |  'hoodie.compact.inline' = 'false',
           |  'hoodie.clustering.inline' = 'false'
           |) $partitionClause AS SELECT * FROM source_data""".stripMargin)
    }
  }

  println("\n--- Write insert Summary ---")
  printStats("COW insert", cowInsertTimes)
  printStats("MOR insert", morInsertTimes)

  // -------------------------------------------------------------------------
  // 5c: Read Benchmark — full scan
  // -------------------------------------------------------------------------

  println("\n=== READ BENCHMARK (full scan) ===")

  // Use tables from bulk_insert benchmark
  val cowReadTable = s"${cowBulkInsert}_1"
  val morReadTable = s"${morBulkInsert}_1"

  // Warmup
  println("--- Warmup ---")
  spark.conf.set("hoodie.datasource.read.use.v2", "false")
  timed("Warmup COW DSv1 read") {
    spark.sql(s"SELECT * FROM $cowReadTable LIMIT 100000").write.format("noop").mode("overwrite").save()
  }
  timed("Warmup MOR DSv1 read") {
    spark.sql(s"SELECT * FROM $morReadTable LIMIT 100000").write.format("noop").mode("overwrite").save()
  }

  if (dsv2Enabled) {
    spark.conf.set("hoodie.datasource.read.use.v2", "true")
    timed("Warmup COW DSv2 read") {
      spark.sql(s"SELECT * FROM $cowReadTable LIMIT 100000").write.format("noop").mode("overwrite").save()
    }
    timed("Warmup MOR DSv2 read") {
      spark.sql(s"SELECT * FROM $morReadTable LIMIT 100000").write.format("noop").mode("overwrite").save()
    }
  }

  println("\n--- Timed iterations ---")

  // DSv1 reads
  spark.conf.set("hoodie.datasource.read.use.v2", "false")

  readFullCowDsv1Times = (1 to iterations).map { i =>
    timed(s"Read full COW DSv1 (iter $i)") {
      spark.sql(s"SELECT * FROM $cowReadTable").write.format("noop").mode("overwrite").save()
    }
  }

  readFullMorDsv1Times = (1 to iterations).map { i =>
    timed(s"Read full MOR DSv1 (iter $i)") {
      spark.sql(s"SELECT * FROM $morReadTable").write.format("noop").mode("overwrite").save()
    }
  }

  // DSv2 reads (if enabled)
  if (dsv2Enabled) {
    spark.conf.set("hoodie.datasource.read.use.v2", "true")

    readFullCowDsv2Times = (1 to iterations).map { i =>
      timed(s"Read full COW DSv2 (iter $i)") {
        spark.sql(s"SELECT * FROM $cowReadTable").write.format("noop").mode("overwrite").save()
      }
    }

    readFullMorDsv2Times = (1 to iterations).map { i =>
      timed(s"Read full MOR DSv2 (iter $i)") {
        spark.sql(s"SELECT * FROM $morReadTable").write.format("noop").mode("overwrite").save()
      }
    }
  }

  println("\n--- Full Scan Summary ---")
  printStats("Read full (COW, DSv1)", readFullCowDsv1Times)
  printStats("Read full (MOR, DSv1)", readFullMorDsv1Times)
  if (dsv2Enabled) {
    printStats("Read full (COW, DSv2)", readFullCowDsv2Times)
    printStats("Read full (MOR, DSv2)", readFullMorDsv2Times)
  }

  // -------------------------------------------------------------------------
  // 5d: Read Benchmark — projected
  // -------------------------------------------------------------------------

  println(s"\n=== READ BENCHMARK (projected: ${projectedCols.split(",").length} columns) ===")

  println("--- Timed iterations ---")

  // DSv1 projected reads
  spark.conf.set("hoodie.datasource.read.use.v2", "false")

  readProjCowDsv1Times = (1 to iterations).map { i =>
    timed(s"Read projected COW DSv1 (iter $i)") {
      spark.sql(s"SELECT $projectedCols FROM $cowReadTable").write.format("noop").mode("overwrite").save()
    }
  }

  readProjMorDsv1Times = (1 to iterations).map { i =>
    timed(s"Read projected MOR DSv1 (iter $i)") {
      spark.sql(s"SELECT $projectedCols FROM $morReadTable").write.format("noop").mode("overwrite").save()
    }
  }

  // DSv2 projected reads (if enabled)
  if (dsv2Enabled) {
    spark.conf.set("hoodie.datasource.read.use.v2", "true")

    readProjCowDsv2Times = (1 to iterations).map { i =>
      timed(s"Read projected COW DSv2 (iter $i)") {
        spark.sql(s"SELECT $projectedCols FROM $cowReadTable").write.format("noop").mode("overwrite").save()
      }
    }

    readProjMorDsv2Times = (1 to iterations).map { i =>
      timed(s"Read projected MOR DSv2 (iter $i)") {
        spark.sql(s"SELECT $projectedCols FROM $morReadTable").write.format("noop").mode("overwrite").save()
      }
    }
  }

  println("\n--- Projected Read Summary ---")
  printStats("Read projected (COW, DSv1)", readProjCowDsv1Times)
  printStats("Read projected (MOR, DSv1)", readProjMorDsv1Times)
  if (dsv2Enabled) {
    printStats("Read projected (COW, DSv2)", readProjCowDsv2Times)
    printStats("Read projected (MOR, DSv2)", readProjMorDsv2Times)
  }

  // -------------------------------------------------------------------------
  // 5e: Compaction Benchmark — MOR only
  // -------------------------------------------------------------------------

  println("\n=== COMPACTION BENCHMARK (MOR) ===")

  // Warmup
  println("--- Warmup ---")
  val warmupCompact = "mor_compact_warmup"
  spark.sql(s"DROP TABLE IF EXISTS $warmupCompact PURGE")
  spark.sql("SET hoodie.datasource.write.operation = bulk_insert")
  spark.sql(
    s"""CREATE TABLE $warmupCompact USING hudi
       |TBLPROPERTIES (
       |  type = 'mor',
       |  primaryKey = '$recordKey',
       |  preCombineField = '$precombineField',
       |  $partitionTblProp
       |  'hoodie.compact.inline' = 'false',
       |  'hoodie.parquet.small.file.limit' = '0',
       |  'hoodie.parquet.max.file.size' = '33554432'
       |) $partitionClause AS SELECT * FROM source_data LIMIT 100000""".stripMargin)
  spark.sql("SET hoodie.datasource.write.operation = upsert")
  spark.sql(s"INSERT INTO $warmupCompact SELECT * FROM source_data LIMIT 50000")
  timed("Warmup compaction") {
    spark.sql(s"CALL run_compaction(op => 'schedule', table => '$benchmarkDb.$warmupCompact')")
    spark.sql(s"CALL run_compaction(op => 'run', table => '$benchmarkDb.$warmupCompact')")
  }
  spark.sql(s"DROP TABLE IF EXISTS $warmupCompact PURGE")

  println("\n--- Timed iterations ---")

  compactionTimes = (1 to iterations).map { i =>
    val tableName = s"mor_compact_$i"
    spark.sql(s"DROP TABLE IF EXISTS $tableName PURGE")
    createdTables += tableName

    // Setup (untimed): create MOR table and add log files via upserts
    println(s"  Setting up $tableName...")
    spark.sql("SET hoodie.datasource.write.operation = bulk_insert")
    spark.sql(
      s"""CREATE TABLE $tableName USING hudi
         |TBLPROPERTIES (
         |  type = 'mor',
         |  primaryKey = '$recordKey',
         |  preCombineField = '$precombineField',
         |  $partitionTblProp
         |  'hoodie.compact.inline' = 'false',
         |  'hoodie.parquet.small.file.limit' = '0',
         |  'hoodie.parquet.max.file.size' = '33554432'
         |) $partitionClause AS SELECT * FROM source_data""".stripMargin)
    spark.sql("SET hoodie.datasource.write.operation = upsert")
    spark.sql(s"INSERT INTO $tableName SELECT * FROM source_data LIMIT 500000")
    spark.sql(s"INSERT INTO $tableName SELECT * FROM source_data LIMIT 500000")

    // Timed: schedule + run compaction
    timed(s"Compaction (iter $i)") {
      spark.sql(s"CALL run_compaction(op => 'schedule', table => '$benchmarkDb.$tableName')")
      spark.sql(s"CALL run_compaction(op => 'run', table => '$benchmarkDb.$tableName')")
    }
  }

  println("\n--- Compaction Summary ---")
  printStats("Compaction (MOR)", compactionTimes)

  // -------------------------------------------------------------------------
  // 5f: Clustering Benchmark — COW only
  // -------------------------------------------------------------------------

  println("\n=== CLUSTERING BENCHMARK (COW) ===")

  // Warmup
  println("--- Warmup ---")
  val warmupCluster = "cow_cluster_warmup"
  spark.sql(s"DROP TABLE IF EXISTS $warmupCluster PURGE")
  spark.sql("SET hoodie.datasource.write.operation = bulk_insert")
  spark.sql(
    s"""CREATE TABLE $warmupCluster USING hudi
       |TBLPROPERTIES (
       |  type = 'cow',
       |  primaryKey = '$recordKey',
       |  preCombineField = '$precombineField',
       |  $partitionTblProp
       |  'hoodie.clustering.inline' = 'false',
       |  'hoodie.parquet.small.file.limit' = '0',
       |  'hoodie.parquet.max.file.size' = '33554432'
       |) $partitionClause AS SELECT * FROM source_data LIMIT 100000""".stripMargin)
  timed("Warmup clustering") {
    spark.sql(s"CALL run_clustering(table => '$benchmarkDb.$warmupCluster')")
  }
  spark.sql(s"DROP TABLE IF EXISTS $warmupCluster PURGE")

  println("\n--- Timed iterations ---")

  clusteringTimes = (1 to iterations).map { i =>
    val tableName = s"cow_cluster_$i"
    spark.sql(s"DROP TABLE IF EXISTS $tableName PURGE")
    createdTables += tableName

    // Setup (untimed): create COW table with many small files
    println(s"  Setting up $tableName...")
    spark.sql("SET hoodie.datasource.write.operation = bulk_insert")
    spark.sql(
      s"""CREATE TABLE $tableName USING hudi
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = '$recordKey',
         |  preCombineField = '$precombineField',
         |  $partitionTblProp
         |  'hoodie.clustering.inline' = 'false',
         |  'hoodie.parquet.small.file.limit' = '0',
         |  'hoodie.parquet.max.file.size' = '33554432'
         |) $partitionClause AS SELECT * FROM source_data""".stripMargin)

    // Timed: run clustering
    timed(s"Clustering (iter $i)") {
      spark.sql(s"CALL run_clustering(table => '$benchmarkDb.$tableName')")
    }
  }

  println("\n--- Clustering Summary ---")
  printStats("Clustering (COW)", clusteringTimes)

  // -------------------------------------------------------------------------
  // 5g: Filter Pushdown Read
  // -------------------------------------------------------------------------

  if (dsv2Enabled) {
    println(s"\n=== READ BENCHMARK (filter pushdown: $filterExpr) ===")

    // Warmup
    println("--- Warmup ---")
    spark.conf.set("hoodie.datasource.read.use.v2", "false")
    timed("Warmup COW filter DSv1") {
      spark.sql(s"SELECT $projectedCols FROM $cowReadTable WHERE $filterExpr LIMIT 100000")
        .write.format("noop").mode("overwrite").save()
    }
    timed("Warmup MOR filter DSv1") {
      spark.sql(s"SELECT $projectedCols FROM $morReadTable WHERE $filterExpr LIMIT 100000")
        .write.format("noop").mode("overwrite").save()
    }
    spark.conf.set("hoodie.datasource.read.use.v2", "true")
    timed("Warmup COW filter DSv2") {
      spark.sql(s"SELECT $projectedCols FROM $cowReadTable WHERE $filterExpr LIMIT 100000")
        .write.format("noop").mode("overwrite").save()
    }
    timed("Warmup MOR filter DSv2") {
      spark.sql(s"SELECT $projectedCols FROM $morReadTable WHERE $filterExpr LIMIT 100000")
        .write.format("noop").mode("overwrite").save()
    }

    println("\n--- Timed iterations ---")

    // DSv1 filter reads
    spark.conf.set("hoodie.datasource.read.use.v2", "false")

    readFilterCowDsv1Times = (1 to iterations).map { i =>
      timed(s"Read filter COW DSv1 (iter $i)") {
        spark.sql(s"SELECT $projectedCols FROM $cowReadTable WHERE $filterExpr")
          .write.format("noop").mode("overwrite").save()
      }
    }

    readFilterMorDsv1Times = (1 to iterations).map { i =>
      timed(s"Read filter MOR DSv1 (iter $i)") {
        spark.sql(s"SELECT $projectedCols FROM $morReadTable WHERE $filterExpr")
          .write.format("noop").mode("overwrite").save()
      }
    }

    // DSv2 filter reads
    spark.conf.set("hoodie.datasource.read.use.v2", "true")

    readFilterCowDsv2Times = (1 to iterations).map { i =>
      timed(s"Read filter COW DSv2 (iter $i)") {
        spark.sql(s"SELECT $projectedCols FROM $cowReadTable WHERE $filterExpr")
          .write.format("noop").mode("overwrite").save()
      }
    }

    readFilterMorDsv2Times = (1 to iterations).map { i =>
      timed(s"Read filter MOR DSv2 (iter $i)") {
        spark.sql(s"SELECT $projectedCols FROM $morReadTable WHERE $filterExpr")
          .write.format("noop").mode("overwrite").save()
      }
    }

    println("\n--- Filter Read Summary ---")
    printStats("Read filter (COW, DSv1)", readFilterCowDsv1Times)
    printStats("Read filter (MOR, DSv1)", readFilterMorDsv1Times)
    printStats("Read filter (COW, DSv2)", readFilterCowDsv2Times)
    printStats("Read filter (MOR, DSv2)", readFilterMorDsv2Times)
  }

  // -------------------------------------------------------------------------
  // 5h: Limit Pushdown Read
  // -------------------------------------------------------------------------

  if (dsv2Enabled) {
    println(s"\n=== READ BENCHMARK (limit pushdown: LIMIT $limitValue) ===")

    // Warmup
    println("--- Warmup ---")
    spark.conf.set("hoodie.datasource.read.use.v2", "false")
    timed("Warmup COW limit DSv1") {
      spark.sql(s"SELECT * FROM $cowReadTable LIMIT 100").write.format("noop").mode("overwrite").save()
    }
    timed("Warmup MOR limit DSv1") {
      spark.sql(s"SELECT * FROM $morReadTable LIMIT 100").write.format("noop").mode("overwrite").save()
    }
    spark.conf.set("hoodie.datasource.read.use.v2", "true")
    timed("Warmup COW limit DSv2") {
      spark.sql(s"SELECT * FROM $cowReadTable LIMIT 100").write.format("noop").mode("overwrite").save()
    }
    timed("Warmup MOR limit DSv2") {
      spark.sql(s"SELECT * FROM $morReadTable LIMIT 100").write.format("noop").mode("overwrite").save()
    }

    println("\n--- Timed iterations ---")

    // DSv1 limit reads
    spark.conf.set("hoodie.datasource.read.use.v2", "false")

    readLimitCowDsv1Times = (1 to iterations).map { i =>
      timed(s"Read limit COW DSv1 (iter $i)") {
        spark.sql(s"SELECT * FROM $cowReadTable LIMIT $limitValue")
          .write.format("noop").mode("overwrite").save()
      }
    }

    readLimitMorDsv1Times = (1 to iterations).map { i =>
      timed(s"Read limit MOR DSv1 (iter $i)") {
        spark.sql(s"SELECT * FROM $morReadTable LIMIT $limitValue")
          .write.format("noop").mode("overwrite").save()
      }
    }

    // DSv2 limit reads
    spark.conf.set("hoodie.datasource.read.use.v2", "true")

    readLimitCowDsv2Times = (1 to iterations).map { i =>
      timed(s"Read limit COW DSv2 (iter $i)") {
        spark.sql(s"SELECT * FROM $cowReadTable LIMIT $limitValue")
          .write.format("noop").mode("overwrite").save()
      }
    }

    readLimitMorDsv2Times = (1 to iterations).map { i =>
      timed(s"Read limit MOR DSv2 (iter $i)") {
        spark.sql(s"SELECT * FROM $morReadTable LIMIT $limitValue")
          .write.format("noop").mode("overwrite").save()
      }
    }

    println("\n--- Limit Read Summary ---")
    printStats("Read limit (COW, DSv1)", readLimitCowDsv1Times)
    printStats("Read limit (MOR, DSv1)", readLimitMorDsv1Times)
    printStats("Read limit (COW, DSv2)", readLimitCowDsv2Times)
    printStats("Read limit (MOR, DSv2)", readLimitMorDsv2Times)
  }

  // -------------------------------------------------------------------------
  // 5i: Aggregate Pushdown
  // -------------------------------------------------------------------------

  if (dsv2Enabled) {
    println("\n=== READ BENCHMARK (aggregate pushdown) ===")

    val cowAgg = "cow_agg"
    spark.sql(s"DROP TABLE IF EXISTS $cowAgg PURGE")
    createdTables += cowAgg

    println("  Creating COW table with column stats metadata...")
    spark.sql("SET hoodie.datasource.write.operation = bulk_insert")
    spark.sql(
      s"""CREATE TABLE $cowAgg USING hudi
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = '$recordKey',
         |  preCombineField = '$precombineField',
         |  $partitionTblProp
         |  'hoodie.metadata.index.column.stats.enable' = 'true',
         |  'hoodie.bulkinsert.shuffle.parallelism' = '200',
         |  'hoodie.compact.inline' = 'false',
         |  'hoodie.clustering.inline' = 'false'
         |) $partitionClause AS SELECT * FROM source_data""".stripMargin)

    // Warmup
    println("--- Warmup ---")
    spark.conf.set("hoodie.datasource.read.use.v2", "false")
    timed("Warmup COUNT(*) DSv1") {
      spark.sql(s"SELECT COUNT(*) FROM $cowAgg").write.format("noop").mode("overwrite").save()
    }
    spark.conf.set("hoodie.datasource.read.use.v2", "true")
    timed("Warmup COUNT(*) DSv2") {
      spark.sql(s"SELECT COUNT(*) FROM $cowAgg").write.format("noop").mode("overwrite").save()
    }

    println("\n--- Timed iterations (COUNT(*)) ---")

    // DSv1 COUNT(*)
    spark.conf.set("hoodie.datasource.read.use.v2", "false")
    aggCountDsv1Times = (1 to iterations).map { i =>
      timed(s"COUNT(*) DSv1 (iter $i)") {
        spark.sql(s"SELECT COUNT(*) FROM $cowAgg").write.format("noop").mode("overwrite").save()
      }
    }

    // DSv2 COUNT(*)
    spark.conf.set("hoodie.datasource.read.use.v2", "true")
    aggCountDsv2Times = (1 to iterations).map { i =>
      timed(s"COUNT(*) DSv2 (iter $i)") {
        spark.sql(s"SELECT COUNT(*) FROM $cowAgg").write.format("noop").mode("overwrite").save()
      }
    }

    println("\n--- Aggregate COUNT(*) Summary ---")
    printStats("COUNT(*) (DSv1)", aggCountDsv1Times)
    printStats("COUNT(*) (DSv2)", aggCountDsv2Times)

    // MIN/MAX — only if filter column is numeric
    if (filterIsNumeric) {
      println(s"\n--- Timed iterations (MIN/MAX on $filterCol) ---")

      spark.conf.set("hoodie.datasource.read.use.v2", "false")
      aggMinMaxDsv1Times = (1 to iterations).map { i =>
        timed(s"MIN/MAX DSv1 (iter $i)") {
          spark.sql(s"SELECT MIN($filterCol), MAX($filterCol) FROM $cowAgg")
            .write.format("noop").mode("overwrite").save()
        }
      }

      spark.conf.set("hoodie.datasource.read.use.v2", "true")
      aggMinMaxDsv2Times = (1 to iterations).map { i =>
        timed(s"MIN/MAX DSv2 (iter $i)") {
          spark.sql(s"SELECT MIN($filterCol), MAX($filterCol) FROM $cowAgg")
            .write.format("noop").mode("overwrite").save()
        }
      }

      println("\n--- Aggregate MIN/MAX Summary ---")
      printStats("MIN/MAX (DSv1)", aggMinMaxDsv1Times)
      printStats("MIN/MAX (DSv2)", aggMinMaxDsv2Times)
    }
  }

  // -------------------------------------------------------------------------
  // 5j: Incremental Read
  // -------------------------------------------------------------------------

  if (dsv2Enabled) {
    println("\n=== READ BENCHMARK (incremental) ===")

    val cowIncr = "cow_incr"
    spark.sql(s"DROP TABLE IF EXISTS $cowIncr PURGE")
    createdTables += cowIncr

    println("  Creating COW table for incremental read...")
    spark.sql("SET hoodie.datasource.write.operation = bulk_insert")
    spark.sql(
      s"""CREATE TABLE $cowIncr USING hudi
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = '$recordKey',
         |  preCombineField = '$precombineField',
         |  $partitionTblProp
         |  'hoodie.bulkinsert.shuffle.parallelism' = '200',
         |  'hoodie.compact.inline' = 'false',
         |  'hoodie.clustering.inline' = 'false'
         |) $partitionClause AS SELECT * FROM source_data""".stripMargin)

    val incrTablePath = getTablePath(cowIncr)
    val commit1Time = getLastCompletionTime(incrTablePath)
    println(s"  Commit 1 completion time: $commit1Time")

    println("  Inserting subset for incremental delta...")
    spark.sql("SET hoodie.datasource.write.operation = insert")
    spark.sql(s"INSERT INTO $cowIncr SELECT * FROM source_data LIMIT 500000")
    val commit2Time = getLastCompletionTime(incrTablePath)
    println(s"  Commit 2 completion time: $commit2Time")

    // Warmup
    println("--- Warmup ---")
    timed("Warmup incremental DSv1") {
      spark.read.format("hudi")
        .option("hoodie.datasource.query.type", "incremental")
        .option("hoodie.datasource.read.begin.instanttime", commit1Time)
        .load(incrTablePath)
        .limit(1000)
        .write.format("noop").mode("overwrite").save()
    }
    timed("Warmup incremental DSv2") {
      spark.read.format("hudi_v2")
        .option("hoodie.datasource.query.type", "incremental")
        .option("hoodie.datasource.read.begin.instanttime", commit1Time)
        .load(incrTablePath)
        .limit(1000)
        .write.format("noop").mode("overwrite").save()
    }

    println("\n--- Timed iterations ---")

    // DSv1 incremental
    incrReadDsv1Times = (1 to iterations).map { i =>
      timed(s"Incremental read DSv1 (iter $i)") {
        spark.read.format("hudi")
          .option("hoodie.datasource.query.type", "incremental")
          .option("hoodie.datasource.read.begin.instanttime", commit1Time)
          .load(incrTablePath)
          .write.format("noop").mode("overwrite").save()
      }
    }

    // DSv2 incremental
    incrReadDsv2Times = (1 to iterations).map { i =>
      timed(s"Incremental read DSv2 (iter $i)") {
        spark.read.format("hudi_v2")
          .option("hoodie.datasource.query.type", "incremental")
          .option("hoodie.datasource.read.begin.instanttime", commit1Time)
          .load(incrTablePath)
          .write.format("noop").mode("overwrite").save()
      }
    }

    println("\n--- Incremental Read Summary ---")
    printStats("Incremental (DSv1)", incrReadDsv1Times)
    printStats("Incremental (DSv2)", incrReadDsv2Times)
  }

  // -------------------------------------------------------------------------
  // 5k: CDC Read
  // -------------------------------------------------------------------------

  if (dsv2Enabled) {
    println("\n=== READ BENCHMARK (CDC) ===")

    val cowCdc = "cow_cdc"
    spark.sql(s"DROP TABLE IF EXISTS $cowCdc PURGE")
    createdTables += cowCdc

    println("  Creating COW table with CDC enabled...")
    spark.sql("SET hoodie.datasource.write.operation = bulk_insert")
    spark.sql(
      s"""CREATE TABLE $cowCdc USING hudi
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = '$recordKey',
         |  preCombineField = '$precombineField',
         |  $partitionTblProp
         |  'hoodie.table.cdc.enabled' = 'true',
         |  'hoodie.table.cdc.supplemental.logging.mode' = 'DATA_BEFORE_AFTER',
         |  'hoodie.bulkinsert.shuffle.parallelism' = '200',
         |  'hoodie.compact.inline' = 'false',
         |  'hoodie.clustering.inline' = 'false'
         |) $partitionClause AS SELECT * FROM source_data""".stripMargin)

    val cdcTablePath = getTablePath(cowCdc)
    val cdcCommit1Time = getLastCompletionTime(cdcTablePath)
    println(s"  Commit 1 completion time: $cdcCommit1Time")

    println("  Inserting subset for CDC delta...")
    spark.sql("SET hoodie.datasource.write.operation = insert")
    spark.sql(s"INSERT INTO $cowCdc SELECT * FROM source_data LIMIT 500000")
    println(s"  Commit 2 completion time: ${getLastCompletionTime(cdcTablePath)}")

    // Warmup
    println("--- Warmup ---")
    timed("Warmup CDC DSv1") {
      spark.read.format("hudi")
        .option("hoodie.datasource.query.type", "incremental")
        .option("hoodie.datasource.query.incremental.format", "cdc")
        .option("hoodie.datasource.read.begin.instanttime", cdcCommit1Time)
        .load(cdcTablePath)
        .limit(1000)
        .write.format("noop").mode("overwrite").save()
    }
    timed("Warmup CDC DSv2") {
      spark.read.format("hudi_v2")
        .option("hoodie.datasource.query.type", "incremental")
        .option("hoodie.datasource.query.incremental.format", "cdc")
        .option("hoodie.datasource.read.begin.instanttime", cdcCommit1Time)
        .load(cdcTablePath)
        .limit(1000)
        .write.format("noop").mode("overwrite").save()
    }

    println("\n--- Timed iterations ---")

    // DSv1 CDC
    cdcReadDsv1Times = (1 to iterations).map { i =>
      timed(s"CDC read DSv1 (iter $i)") {
        spark.read.format("hudi")
          .option("hoodie.datasource.query.type", "incremental")
          .option("hoodie.datasource.query.incremental.format", "cdc")
          .option("hoodie.datasource.read.begin.instanttime", cdcCommit1Time)
          .load(cdcTablePath)
          .write.format("noop").mode("overwrite").save()
      }
    }

    // DSv2 CDC
    cdcReadDsv2Times = (1 to iterations).map { i =>
      timed(s"CDC read DSv2 (iter $i)") {
        spark.read.format("hudi_v2")
          .option("hoodie.datasource.query.type", "incremental")
          .option("hoodie.datasource.query.incremental.format", "cdc")
          .option("hoodie.datasource.read.begin.instanttime", cdcCommit1Time)
          .load(cdcTablePath)
          .write.format("noop").mode("overwrite").save()
      }
    }

    println("\n--- CDC Read Summary ---")
    printStats("CDC read (DSv1)", cdcReadDsv1Times)
    printStats("CDC read (DSv2)", cdcReadDsv2Times)
  }

  // -------------------------------------------------------------------------
  // Section 6: Final Summary
  // -------------------------------------------------------------------------

  println("\n" + "=" * 60)
  println("BENCHMARK COMPLETE")
  println("=" * 60)
  println()
  printStats("Write bulk_insert (COW)      ", cowBulkInsertTimes)
  printStats("Write bulk_insert (MOR)      ", morBulkInsertTimes)
  printStats("Write insert (COW)           ", cowInsertTimes)
  printStats("Write insert (MOR)           ", morInsertTimes)
  printStats("Read full (COW, DSv1)        ", readFullCowDsv1Times)
  printStats("Read full (MOR, DSv1)        ", readFullMorDsv1Times)
  if (dsv2Enabled) {
    printStats("Read full (COW, DSv2)        ", readFullCowDsv2Times)
    printStats("Read full (MOR, DSv2)        ", readFullMorDsv2Times)
  } else {
    println("Read full (COW, DSv2)        : N/A  (DSv2 disabled)")
    println("Read full (MOR, DSv2)        : N/A  (DSv2 disabled)")
  }
  printStats("Read projected (COW, DSv1)   ", readProjCowDsv1Times)
  printStats("Read projected (MOR, DSv1)   ", readProjMorDsv1Times)
  if (dsv2Enabled) {
    printStats("Read projected (COW, DSv2)   ", readProjCowDsv2Times)
    printStats("Read projected (MOR, DSv2)   ", readProjMorDsv2Times)
  } else {
    println("Read projected (COW, DSv2)   : N/A  (DSv2 disabled)")
    println("Read projected (MOR, DSv2)   : N/A  (DSv2 disabled)")
  }
  printStats("Compaction (MOR)             ", compactionTimes)
  printStats("Clustering (COW)             ", clusteringTimes)
  if (dsv2Enabled) {
    printStats("Read filter (COW, DSv1)      ", readFilterCowDsv1Times)
    printStats("Read filter (MOR, DSv1)      ", readFilterMorDsv1Times)
    printStats("Read filter (COW, DSv2)      ", readFilterCowDsv2Times)
    printStats("Read filter (MOR, DSv2)      ", readFilterMorDsv2Times)
    printStats("Read limit (COW, DSv1)       ", readLimitCowDsv1Times)
    printStats("Read limit (MOR, DSv1)       ", readLimitMorDsv1Times)
    printStats("Read limit (COW, DSv2)       ", readLimitCowDsv2Times)
    printStats("Read limit (MOR, DSv2)       ", readLimitMorDsv2Times)
    printStats("Aggregate COUNT(*) (DSv1)    ", aggCountDsv1Times)
    printStats("Aggregate COUNT(*) (DSv2)    ", aggCountDsv2Times)
    if (aggMinMaxDsv1Times.nonEmpty) {
      printStats("Aggregate MIN/MAX (DSv1)     ", aggMinMaxDsv1Times)
      printStats("Aggregate MIN/MAX (DSv2)     ", aggMinMaxDsv2Times)
    }
    printStats("Incremental read (DSv1)      ", incrReadDsv1Times)
    printStats("Incremental read (DSv2)      ", incrReadDsv2Times)
    printStats("CDC read (DSv1)              ", cdcReadDsv1Times)
    printStats("CDC read (DSv2)              ", cdcReadDsv2Times)
  }

  // -------------------------------------------------------------------------
  // Performance Comparison: DSv2 vs DSv1
  // -------------------------------------------------------------------------

  if (dsv2Enabled) {
    println("\n" + "=" * 60)
    println("DSv2 vs DSv1 PERFORMANCE COMPARISON")
    println("=" * 60)

    def comparePerf(label: String, dsv1Times: Seq[Double], dsv2Times: Seq[Double]): Boolean = {
      if (dsv1Times.isEmpty || dsv2Times.isEmpty) {
        println(f"$label%-35s: N/A (no data)")
        return false
      }
      val dsv1Avg = dsv1Times.sum / dsv1Times.size
      val dsv2Avg = dsv2Times.sum / dsv2Times.size
      val speedup = dsv1Avg / dsv2Avg
      val winner = if (speedup >= 1.0) "DSv2 FASTER" else "DSv1 FASTER"
      println(f"$label%-35s: DSv1 avg ${dsv1Avg}%.1fs, DSv2 avg ${dsv2Avg}%.1fs, speedup ${speedup}%.2fx ($winner)")
      speedup >= 1.0
    }

    println()
    var dsv2Wins = 0
    var totalComparisons = 0

    val comparisons = Seq(
      ("Full scan (COW)", readFullCowDsv1Times, readFullCowDsv2Times),
      ("Full scan (MOR)", readFullMorDsv1Times, readFullMorDsv2Times),
      ("Projected (COW)", readProjCowDsv1Times, readProjCowDsv2Times),
      ("Projected (MOR)", readProjMorDsv1Times, readProjMorDsv2Times),
      ("Filter (COW)", readFilterCowDsv1Times, readFilterCowDsv2Times),
      ("Filter (MOR)", readFilterMorDsv1Times, readFilterMorDsv2Times),
      ("Limit (COW)", readLimitCowDsv1Times, readLimitCowDsv2Times),
      ("Limit (MOR)", readLimitMorDsv1Times, readLimitMorDsv2Times),
      ("Aggregate COUNT(*)", aggCountDsv1Times, aggCountDsv2Times),
      ("Aggregate MIN/MAX", aggMinMaxDsv1Times, aggMinMaxDsv2Times),
      ("Incremental", incrReadDsv1Times, incrReadDsv2Times),
      ("CDC", cdcReadDsv1Times, cdcReadDsv2Times)
    )

    comparisons.foreach { case (label, dsv1, dsv2) =>
      if (dsv1.nonEmpty && dsv2.nonEmpty) {
        totalComparisons += 1
        if (comparePerf(label, dsv1, dsv2)) dsv2Wins += 1
      }
    }

    println()
    if (dsv2Wins > 0) {
      println(s"PASS: DSv2 is faster than DSv1 in $dsv2Wins of $totalComparisons scenarios")
    } else {
      println(s"WARNING: DSv2 was not faster than DSv1 in any scenario (0 of $totalComparisons)")
    }
  }

} finally {
  // -------------------------------------------------------------------------
  // Section 7: Cleanup
  // -------------------------------------------------------------------------

  println("\n--- Cleanup ---")
  createdTables.foreach { table =>
    try {
      spark.sql(s"DROP TABLE IF EXISTS $table PURGE")
      println(s"  Dropped $table")
    } catch {
      case e: Exception => println(s"  Warning: failed to drop $table: ${e.getMessage}")
    }
  }

  // Drop the benchmark database (CASCADE drops any remaining tables)
  try {
    spark.sql(s"DROP DATABASE IF EXISTS $benchmarkDb CASCADE")
    println(s"  Dropped database $benchmarkDb")
  } catch {
    case e: Exception => println(s"  Warning: failed to drop database $benchmarkDb: ${e.getMessage}")
  }

  // Unpersist source data
  try {
    spark.catalog.uncacheTable("source_data")
    println("  Unpersisted source_data")
  } catch {
    case e: Exception => println(s"  Warning: failed to unpersist source_data: ${e.getMessage}")
  }

  // Restore original stdout and close log file
  System.out.flush()
  Console.setOut(origOut)
  System.setOut(origOut)
  logFile.close()
  println(s"Benchmark log saved to: $logFileName")
}

// Exit spark-shell after benchmark
System.exit(0)
