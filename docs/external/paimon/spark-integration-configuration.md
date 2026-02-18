# Spark Integration Configuration

## Maven Build Profiles

### `spark3` (default)

- **Scala version:** 2.12
- **Spark versions:** 3.2, 3.3, 3.4, 3.5
- **JDK requirement:** 8+
- **Modules activated:** `paimon-spark3-common`, `paimon-spark-3.2`, `paimon-spark-3.3`, `paimon-spark-3.4`, `paimon-spark-3.5`, `paimon-spark-ut`

### `spark4`

- **Scala version:** 2.13
- **Spark versions:** 4.0
- **JDK requirement:** 17
- **Modules activated:** `paimon-spark4-common`, `paimon-spark-4.0`, `paimon-spark-ut`

### Example Build Commands

```bash
# Full build with Spark 3 (default profile)
mvn clean install -DskipTests -Pspark3

# Full build with Spark 4
mvn clean install -DskipTests -Pspark4

# Build a single Spark 3.5 shim module with dependencies
mvn -T 2C -B -ntp clean install -pl paimon-spark/paimon-spark-3.5 -am -DskipTests -Pspark3

# Build the Spark 4.0 shim module with dependencies
mvn -T 2C -B -ntp clean install -pl paimon-spark/paimon-spark-4.0 -am -DskipTests -Pspark4
```

## Connector Options

Options defined in `SparkConnectorOptions` (`org.apache.paimon.spark.SparkConnectorOptions`). These are set as DataSource options when reading from or writing to Paimon tables.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `requiredSparkConfsCheck.enabled` | Boolean | `true` | Whether to verify SparkSession is initialized with required configurations. |
| `write.merge-schema` | Boolean | `false` | If true, merge the data schema and the table schema automatically before write data. |
| `write.merge-schema.explicit-cast` | Boolean | `false` | If true, allow to merge data types if the two types meet the rules for explicit casting. |
| `write.use-v2-write` | Boolean | `false` | If true, v2 write will be used. Currently, only HASH_FIXED and BUCKET_UNAWARE bucket modes are supported. Will fall back to v1 write for other bucket modes. Currently, Spark V2 write does not support TableCapability.STREAMING_WRITE. |
| `read.stream.maxFilesPerTrigger` | Integer | *(none)* | The maximum number of files returned in a single batch. |
| `read.stream.maxBytesPerTrigger` | Long | *(none)* | The maximum number of bytes returned in a single batch. |
| `read.stream.maxRowsPerTrigger` | Long | *(none)* | The maximum number of rows returned in a single batch. |
| `read.stream.minRowsPerTrigger` | Long | *(none)* | The minimum number of rows returned in a single batch, which used to create MinRowsReadLimit with `read.stream.maxTriggerDelayMs` together. |
| `read.stream.maxTriggerDelayMs` | Long | *(none)* | The maximum delay between two adjacent batches, which used to create MinRowsReadLimit with `read.stream.minRowsPerTrigger` together. |
| `read.changelog` | Boolean | `false` | Whether to read row in the form of changelog (add rowkind column in row to represent its change type). |
| `read.allow.fullScan` | Boolean | `true` | Whether to allow full scan when reading a partitioned table. |
| `source.split.target-size-with-column-pruning` | Boolean | `false` | Whether to adjust the target split size based on pruned (projected) columns. If enabled, split size estimation uses only the columns actually being read. |

## Catalog Options

Options defined in `SparkCatalogOptions` (`org.apache.paimon.spark.SparkCatalogOptions`). These are set as Spark catalog configuration properties.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `catalog.create-underlying-session-catalog` | Boolean | `false` | If true, create and use an underlying session catalog instead of default session catalog when use SparkGenericCatalog. |
| `defaultDatabase` | String | `"default"` | The default database name. |
| `v1Function.enabled` | Boolean | `true` | Whether to enable v1 function. |

## Catalog Setup

### SparkCatalog (Named Catalog)

Use `SparkCatalog` when Paimon is a named catalog alongside other catalogs:

```properties
spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog
spark.sql.catalog.paimon.warehouse=s3://bucket/warehouse
```

Access tables via the catalog name:

```sql
SELECT * FROM paimon.db.table;
```

### SparkGenericCatalog (Session Catalog)

Use `SparkGenericCatalog` when Paimon should be the default session catalog (`spark_catalog`). Non-Paimon tables are delegated to the underlying Hive or in-memory session catalog:

```properties
spark.sql.catalog.spark_catalog=org.apache.paimon.spark.SparkGenericCatalog
spark.sql.catalog.spark_catalog.warehouse=s3://bucket/warehouse
```

Access tables directly without a catalog prefix:

```sql
SELECT * FROM db.table;
```

Set `catalog.create-underlying-session-catalog=true` to explicitly create and use an underlying session catalog instead of the default.

## SQL Extensions Setup

Register `PaimonSparkSessionExtensions` to enable Paimon's custom SQL syntax (CALL procedures, MERGE INTO, etc.), analysis rules, optimizer rules, and planner strategies:

```properties
spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions
```

This injects:
- Custom SQL parser for Paimon-specific syntax
- Catalyst analysis rules (`PaimonAnalysis`, `PaimonMergeInto`, `PaimonProcedureResolver`, etc.)
- Optimizer rules (`MergePaimonScalarSubqueries`, `OptimizeMetadataOnlyDeleteFromPaimonTable`)
- Planner strategies (`PaimonStrategy`, `OldCompatibleStrategy`)

## Shading and Packaging

The Spark integration uses a cascading shade strategy where each layer bundles the previous one:

1. **`paimon-bundle`** -- Bundles `paimon-api` + `paimon-common` + `paimon-core` + `paimon-format` into a single shaded JAR.
2. **`paimon-spark-common`** -- Shades and bundles `paimon-bundle` to produce a self-contained artifact.
3. **`paimon-spark3-common` / `paimon-spark4-common`** -- Shades and bundles both `paimon-bundle` and `paimon-spark-common`.
4. **Version-specific shim** (e.g. `paimon-spark-3.5`) -- Shades and bundles its parent major-version-common module.

The final shim JAR is self-contained and deploy-ready. Drop it into Spark's `jars/` directory or pass it via `--jars` / `--packages`.

## DataSource Registration

Paimon registers its data source and shim implementations via SPI (Service Provider Interface) files:

### SparkSource

- **SPI file:** `paimon-spark-common/src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`
- **Implementation:** `org.apache.paimon.spark.SparkSource`
- **Short name:** `"paimon"`

### SparkShim

- **SPI file (Spark 3):** `paimon-spark3-common/src/main/resources/META-INF/services/org.apache.spark.sql.paimon.shims.SparkShim`
- **Implementation:** `org.apache.spark.sql.paimon.shims.Spark3Shim`

- **SPI file (Spark 4):** `paimon-spark4-common/src/main/resources/META-INF/services/org.apache.spark.sql.paimon.shims.SparkShim`
- **Implementation:** `org.apache.spark.sql.paimon.shims.Spark4Shim`

## Related Documentation

- [Spark Integration Overview](spark-integration-overview.md) -- supported versions, module summary, key capabilities
- [Spark Integration Architecture](spark-integration-architecture.md) -- module structure, shim pattern, key classes, and testing strategy
