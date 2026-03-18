# Implementing a Custom Flink Table Connector (Source & Sink)

This guide explains how to build a custom connector that integrates with Flink's
Table/SQL API as both a **Source** and a **Sink**. After following this guide you
will have a connector JAR that enables SQL like:

```sql
CREATE TABLE my_table (
    id    BIGINT,
    name  STRING,
    ts    TIMESTAMP(3)
) WITH (
    'connector' = 'my-connector',
    'hostname'  = 'localhost',
    'port'      = '9090'
);

-- read
SELECT * FROM my_table;

-- write
INSERT INTO my_table SELECT id, name, ts FROM other_table;
```

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Project Setup](#2-project-setup)
3. [Define Connector Options](#3-define-connector-options)
4. [Implement the Source](#4-implement-the-source)
5. [Implement the Sink](#5-implement-the-sink)
6. [Create the Factory](#6-create-the-factory)
7. [Register via SPI](#7-register-via-spi)
8. [Optional: Pluggable Formats](#8-optional-pluggable-formats)
9. [Optional: Push-Down Abilities](#9-optional-push-down-abilities)
10. [Testing](#10-testing)
11. [Packaging and Deployment](#11-packaging-and-deployment)
12. [Complete Example](#12-complete-example)
13. [Reference](#13-reference)

---

## 1. Architecture Overview

Flink discovers connectors at runtime through Java's `ServiceLoader` mechanism.
The overall class hierarchy is:

```
Factory                              (SPI entry point)
 └── DynamicTableFactory             (provides Context with schema + options)
       ├── DynamicTableSourceFactory
       │     └── creates DynamicTableSource
       │           └── ScanTableSource        (streaming/batch reads)
       │                 └── provides ScanRuntimeProvider
       │                       └── SourceProvider (wraps Source<RowData>)
       └── DynamicTableSinkFactory
             └── creates DynamicTableSink
                   └── provides SinkRuntimeProvider
                         └── SinkV2Provider (wraps Sink<RowData>)
```

**Lifecycle:**

1. User creates a table with `CREATE TABLE ... WITH ('connector' = 'my-connector', ...)`.
2. Flink stores the table definition (schema + options) in the catalog.
3. At query planning time, Flink loads all `Factory` implementations via SPI,
   finds the one whose `factoryIdentifier()` matches the `'connector'` value.
4. Flink calls `createDynamicTableSource(context)` or `createDynamicTableSink(context)`.
5. The planner inspects the returned object for push-down abilities
   (`SupportsFilterPushDown`, `SupportsProjectionPushDown`, etc.) and applies
   optimizations.
6. The planner calls `getScanRuntimeProvider()` / `getSinkRuntimeProvider()` to
   obtain the runtime operator that will run on TaskManagers.

---

## 2. Project Setup

### Maven Dependencies

```xml
<dependencies>
    <!-- Core Table API interfaces (compile-time only) -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-table-common</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
    </dependency>

    <!-- For SourceProvider / SinkV2Provider -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-core</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
    </dependency>

    <!-- Testing -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-test-utils</artifactId>
        <version>${flink.version}</version>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-table-planner-loader</artifactId>
        <version>${flink.version}</version>
        <scope>test</scope>
    </dependency>
</dependencies>
```

Mark Flink dependencies as `provided` — they are supplied by the cluster at
runtime. Your connector JAR should be a fat-jar containing only your code and
any third-party libraries that Flink does not already provide.

### Recommended Package Structure

```
com.example.flink.connector.myconnector/
├── MyConnectorOptions.java           # ConfigOption constants
├── MyConnectorFactory.java           # DynamicTableSourceFactory + DynamicTableSinkFactory
├── MyConnectorSource.java            # ScanTableSource implementation
├── MyConnectorSink.java              # DynamicTableSink implementation
├── source/
│   ├── MySource.java                 # Source<RowData, ...> (FLIP-27 API)
│   ├── MySplit.java                  # SourceSplit
│   ├── MySplitEnumerator.java        # SplitEnumerator
│   └── MySourceReader.java           # SourceReader
└── sink/
    ├── MySink.java                   # Sink<RowData> (FLIP-191 API)
    ├── MySinkWriter.java             # SinkWriter
    └── MyCommittable.java            # (if two-phase commit is needed)
```

---

## 3. Define Connector Options

Create a class to hold all `ConfigOption` constants your connector accepts.
These options appear in the `WITH (...)` clause of `CREATE TABLE`.

```java
package com.example.flink.connector.myconnector;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class MyConnectorOptions {

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The hostname of the external system.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(9090)
                    .withDescription("The port of the external system.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the table in the external system to "
                                    + "read from / write to.");

    public static final ConfigOption<Integer> SCAN_PARALLELISM =
            ConfigOptions.key("scan.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Parallelism for the source. "
                                    + "If not set, uses the global default.");

    public static final ConfigOption<Integer> SINK_BUFFER_SIZE =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Maximum number of rows to buffer before flushing.");

    private MyConnectorOptions() {}
}
```

**Key `ConfigOptions` builder methods:**

| Method | Purpose |
|---|---|
| `.stringType()`, `.intType()`, `.longType()`, `.booleanType()`, `.durationType()` | Set the value type |
| `.defaultValue(T)` | Provide a default |
| `.noDefaultValue()` | Mark as required (no default) |
| `.withDescription("...")` | Human-readable description |
| `.withFallbackKeys("old-key")` | Backwards-compatible renames |

> **Note:** Do not declare `'connector'` or `'format'` in your options — those
> are framework-level keys managed by `FactoryUtil`.

---

## 4. Implement the Source

### 4.1. `ScanTableSource` — Planning-Time Object

This object is created once at planning time. It describes the source's
capabilities and ultimately provides a runtime operator.

```java
package com.example.flink.connector.myconnector;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.types.DataType;

public class MyConnectorSource implements ScanTableSource {

    private final String hostname;
    private final int port;
    private final String tableName;
    private final DataType producedDataType;
    private final Integer parallelism;

    public MyConnectorSource(
            String hostname,
            int port,
            String tableName,
            DataType producedDataType,
            Integer parallelism) {
        this.hostname = hostname;
        this.port = port;
        this.tableName = tableName;
        this.producedDataType = producedDataType;
        this.parallelism = parallelism;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // For an append-only source (most common):
        return ChangelogMode.insertOnly();
        // For a CDC source that emits inserts, updates, and deletes:
        // return ChangelogMode.all();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        // Create the FLIP-27 Source implementation
        Source<RowData, ?, ?> source =
                new MySource(hostname, port, tableName, producedDataType);

        // SourceProvider.of() wraps it for the table runtime
        if (parallelism != null) {
            return SourceProvider.of(source, parallelism);
        }
        return SourceProvider.of(source);
    }

    @Override
    public DynamicTableSource copy() {
        return new MyConnectorSource(
                hostname, port, tableName, producedDataType, parallelism);
    }

    @Override
    public String asSummaryString() {
        return "MyConnector";
    }
}
```

**`ChangelogMode` options:**

| Mode | Use Case |
|---|---|
| `ChangelogMode.insertOnly()` | Append-only source (logs, files, generated data) |
| `ChangelogMode.upsert()` | INSERT + UPDATE_AFTER + DELETE (CDC without before-image) |
| `ChangelogMode.all()` | Full CDC: INSERT + UPDATE_BEFORE + UPDATE_AFTER + DELETE |

### 4.2. FLIP-27 `Source` — Runtime Implementation

The FLIP-27 Source API is the recommended approach. It separates split
discovery (on the JobManager) from data reading (on TaskManagers).

You need to implement:

- `Source<RowData, MySplit, MyEnumeratorState>` — the entry point
- `SplitEnumerator<MySplit, MyEnumeratorState>` — assigns splits to readers
- `SourceReader<RowData, MySplit>` — reads data from an assigned split
- `SourceSplit` — represents a unit of work (e.g. a partition, a shard)

```java
package com.example.flink.connector.myconnector.source;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

public class MySource
        implements Source<RowData, MySplit, MyEnumeratorState> {

    private final String hostname;
    private final int port;
    private final String tableName;

    public MySource(String hostname, int port, String tableName) {
        this.hostname = hostname;
        this.port = port;
        this.tableName = tableName;
    }

    @Override
    public Boundedness getBoundedness() {
        // Return BOUNDED for batch, CONTINUOUS_UNBOUNDED for streaming
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<MySplit, MyEnumeratorState> createEnumerator(
            SplitEnumeratorContext<MySplit> context) {
        return new MySplitEnumerator(context, hostname, port, tableName);
    }

    @Override
    public SplitEnumerator<MySplit, MyEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<MySplit> context,
            MyEnumeratorState checkpoint) {
        return new MySplitEnumerator(
                context, hostname, port, tableName, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<MySplit> getSplitSerializer() {
        return new MySplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<MyEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new MyEnumeratorStateSerializer();
    }

    @Override
    public SourceReader<RowData, MySplit> createReader(
            SourceReaderContext context) {
        return new MySourceReader(context, hostname, port);
    }
}
```

> For a minimal bounded source that reads a fixed dataset, you can use
> `SingleThreadMultiplexSourceReaderBase` from `flink-connector-base` to
> simplify the `SourceReader` implementation.

---

## 5. Implement the Sink

### 5.1. `DynamicTableSink` — Planning-Time Object

```java
package com.example.flink.connector.myconnector;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.types.DataType;

public class MyConnectorSink implements DynamicTableSink {

    private final String hostname;
    private final int port;
    private final String tableName;
    private final DataType consumedDataType;
    private final int bufferSize;
    private final Integer parallelism;

    public MyConnectorSink(
            String hostname,
            int port,
            String tableName,
            DataType consumedDataType,
            int bufferSize,
            Integer parallelism) {
        this.hostname = hostname;
        this.port = port;
        this.tableName = tableName;
        this.consumedDataType = consumedDataType;
        this.bufferSize = bufferSize;
        this.parallelism = parallelism;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // Accept whatever the planner proposes (for an upsert-capable sink).
        // For an append-only sink, return ChangelogMode.insertOnly() instead.
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        MySink sink = new MySink(hostname, port, tableName, bufferSize);

        if (parallelism != null) {
            return SinkV2Provider.of(sink, parallelism);
        }
        return SinkV2Provider.of(sink);
    }

    @Override
    public DynamicTableSink copy() {
        return new MyConnectorSink(
                hostname, port, tableName, consumedDataType,
                bufferSize, parallelism);
    }

    @Override
    public String asSummaryString() {
        return "MyConnector";
    }
}
```

### 5.2. FLIP-191 `Sink` and `SinkWriter` — Runtime Implementation

```java
package com.example.flink.connector.myconnector.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** The runtime Sink that creates writers for each parallel subtask. */
public class MySink implements Sink<RowData> {

    private final String hostname;
    private final int port;
    private final String tableName;
    private final int bufferSize;

    public MySink(String hostname, int port, String tableName, int bufferSize) {
        this.hostname = hostname;
        this.port = port;
        this.tableName = tableName;
        this.bufferSize = bufferSize;
    }

    @Override
    public SinkWriter<RowData> createWriter(InitContext context)
            throws IOException {
        return new MySinkWriter(hostname, port, tableName, bufferSize);
    }
}

/** Each parallel subtask gets its own writer instance. */
public class MySinkWriter implements SinkWriter<RowData> {

    private final String hostname;
    private final int port;
    private final String tableName;
    private final int bufferSize;
    private final List<RowData> buffer;
    // private ExternalClient client;  // your connection to the external system

    public MySinkWriter(
            String hostname, int port, String tableName, int bufferSize) {
        this.hostname = hostname;
        this.port = port;
        this.tableName = tableName;
        this.bufferSize = bufferSize;
        this.buffer = new ArrayList<>();
        // this.client = new ExternalClient(hostname, port);
    }

    @Override
    public void write(RowData element, Context context)
            throws IOException, InterruptedException {
        buffer.add(element);
        if (buffer.size() >= bufferSize) {
            flush();
        }
    }

    @Override
    public void flush(boolean endOfInput)
            throws IOException, InterruptedException {
        flush();
        if (endOfInput) {
            // Final flush — no more data will arrive
        }
    }

    @Override
    public void close() throws Exception {
        flush();
        // client.close();
    }

    private void flush() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        // client.bulkInsert(tableName, buffer);
        buffer.clear();
    }
}
```

> **Two-phase commit:** If your external system supports transactions, implement
> `TwoPhaseCommittingSink<RowData, MyCommittable>` instead of `Sink<RowData>`.
> This requires implementing a `Committer` that commits or aborts prepared
> transactions during checkpoints.

---

## 6. Create the Factory

The factory is the SPI entry point. It implements both `DynamicTableSourceFactory`
and `DynamicTableSinkFactory` (a single class can provide both source and sink).

```java
package com.example.flink.connector.myconnector;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

import static com.example.flink.connector.myconnector.MyConnectorOptions.*;

public class MyConnectorFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "my-connector";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(TABLE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PORT);
        options.add(SCAN_PARALLELISM);
        options.add(SINK_BUFFER_SIZE);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // Use FactoryUtil helper to validate options
        FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig options = helper.getOptions();
        DataType producedDataType = context.getPhysicalRowDataType();

        return new MyConnectorSource(
                options.get(HOSTNAME),
                options.get(PORT),
                options.get(TABLE_NAME),
                producedDataType,
                options.getOptional(SCAN_PARALLELISM).orElse(null));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig options = helper.getOptions();
        DataType consumedDataType = context.getPhysicalRowDataType();

        return new MyConnectorSink(
                options.get(HOSTNAME),
                options.get(PORT),
                options.get(TABLE_NAME),
                consumedDataType,
                options.get(SINK_BUFFER_SIZE),
                options.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null));
    }
}
```

**Key points:**

- `factoryIdentifier()` returns the string used in `'connector' = '...'`.
- `requiredOptions()` — options that **must** be present in the DDL.
- `optionalOptions()` — options that have defaults or are not always needed.
- `FactoryUtil.createTableFactoryHelper(this, context)` validates that all user-provided
  options are recognized (no typos) and all required options are present.

---

## 7. Register via SPI

Create the service file so that Flink's `ServiceLoader` can discover your factory.

**File:** `src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory`

```
com.example.flink.connector.myconnector.MyConnectorFactory
```

That's it. One fully-qualified class name per line. If your JAR also contains
format factories, list them here as well.

---

## 8. Optional: Pluggable Formats

If your connector works with different serialization formats (like the filesystem
connector supports JSON, CSV, Parquet, etc.), use format discovery.

In your factory, use the helper to discover format factories:

```java
@Override
public DynamicTableSource createDynamicTableSource(Context context) {
    FactoryUtil.TableFactoryHelper helper =
            FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();

    // Discover the deserialization format from the 'format' option
    DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
            helper.discoverDecodingFormat(
                    DeserializationFormatFactory.class,
                    FactoryUtil.FORMAT);

    // Pass the format to your source
    return new MyConnectorSource(..., decodingFormat);
}
```

Then in your source's `getScanRuntimeProvider`, instantiate the format:

```java
DeserializationSchema<RowData> deserializer =
        decodingFormat.createRuntimeDecoder(context, producedDataType);
```

The user would specify the format in DDL:

```sql
CREATE TABLE my_table (...) WITH (
    'connector' = 'my-connector',
    'format'    = 'json',
    ...
);
```

---

## 9. Optional: Push-Down Abilities

Your source and sink can optionally implement ability interfaces to enable
planner optimizations. The planner calls the corresponding `apply*()` method
before requesting the runtime provider.

### Source Abilities

| Interface | What It Does |
|---|---|
| `SupportsProjectionPushDown` | Prune columns — only read needed fields |
| `SupportsFilterPushDown` | Push WHERE clauses to the external system |
| `SupportsLimitPushDown` | Push LIMIT to the external system |
| `SupportsPartitionPushDown` | Prune partitions based on query predicates |
| `SupportsWatermarkPushDown` | Inject watermark strategy into the source |
| `SupportsReadingMetadata` | Expose connector metadata as virtual columns |

**Example: Projection Push-Down**

```java
public class MyConnectorSource
        implements ScanTableSource, SupportsProjectionPushDown {

    private int[] projectedFields;

    @Override
    public boolean supportsNestedProjection() {
        return false; // true if your source handles nested field projection
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        // Store the projected fields to use later when reading
        this.projectedFields = new int[projectedFields.length];
        for (int i = 0; i < projectedFields.length; i++) {
            this.projectedFields[i] = projectedFields[i][0];
        }
        this.producedDataType = producedDataType;
    }

    // ... pass projectedFields to your Source implementation
}
```

### Sink Abilities

| Interface | What It Does |
|---|---|
| `SupportsPartitioning` | Accept static partition values from INSERT INTO ... PARTITION |
| `SupportsOverwrite` | Support INSERT OVERWRITE |
| `SupportsWritingMetadata` | Allow writing connector metadata columns |
| `SupportsTruncate` | Support TRUNCATE TABLE |

---

## 10. Testing

### Unit Test with MiniCluster

```java
package com.example.flink.connector.myconnector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MyConnectorITCase {

    @Test
    void testSourceAndSink() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(
                EnvironmentSettings.inStreamingMode());

        // Create source table
        tEnv.executeSql(
                "CREATE TABLE source_table ("
                        + "  id BIGINT,"
                        + "  name STRING"
                        + ") WITH ("
                        + "  'connector' = 'my-connector',"
                        + "  'hostname'  = 'localhost',"
                        + "  'port'      = '9090',"
                        + "  'table-name' = 'test_source'"
                        + ")");

        // Create sink table
        tEnv.executeSql(
                "CREATE TABLE sink_table ("
                        + "  id BIGINT,"
                        + "  name STRING"
                        + ") WITH ("
                        + "  'connector' = 'my-connector',"
                        + "  'hostname'  = 'localhost',"
                        + "  'port'      = '9090',"
                        + "  'table-name' = 'test_sink'"
                        + ")");

        // Run a query
        TableResult result = tEnv.executeSql(
                "INSERT INTO sink_table SELECT * FROM source_table");
        result.await();

        // Verify results ...
    }

    @Test
    void testFactoryDiscovery() {
        // Verify the factory is properly discovered via SPI
        TableEnvironment tEnv = TableEnvironment.create(
                EnvironmentSettings.inStreamingMode());

        // This will fail if the SPI file is missing or the factory
        // identifier doesn't match
        tEnv.executeSql(
                "CREATE TABLE test_table ("
                        + "  id BIGINT"
                        + ") WITH ("
                        + "  'connector'  = 'my-connector',"
                        + "  'hostname'   = 'localhost',"
                        + "  'table-name' = 'test'"
                        + ")");

        tEnv.executeSql("DESCRIBE test_table").print();
    }
}
```

### Testing with DataGen + Print (Quick Validation)

Use built-in connectors to validate your sink in isolation:

```sql
-- Generate test data
CREATE TABLE gen_source (
    id   BIGINT,
    name STRING
) WITH (
    'connector'       = 'datagen',
    'rows-per-second' = '10',
    'number-of-rows'  = '100'
);

-- Write to your connector
CREATE TABLE my_sink (...) WITH ('connector' = 'my-connector', ...);

INSERT INTO my_sink SELECT * FROM gen_source;
```

Or validate your source by piping to `print`:

```sql
CREATE TABLE my_source (...) WITH ('connector' = 'my-connector', ...);

CREATE TABLE print_sink (
    id   BIGINT,
    name STRING
) WITH ('connector' = 'print');

INSERT INTO print_sink SELECT * FROM my_source;
```

---

## 11. Packaging and Deployment

### Build a Fat JAR

Use `maven-shade-plugin` to produce a single JAR with your connector code and
any third-party dependencies (exclude Flink's own libraries):

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <executions>
        <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
                <artifactSet>
                    <excludes>
                        <exclude>org.apache.flink:*</exclude>
                        <exclude>org.slf4j:*</exclude>
                    </excludes>
                </artifactSet>
            </configuration>
        </execution>
    </executions>
</plugin>
```

### Deploy

Place the connector JAR in one of:

- **`$FLINK_HOME/lib/`** — always on the classpath
- **`$FLINK_HOME/plugins/my-connector/`** — loaded via Flink's plugin mechanism
  (recommended for better isolation)
- **SQL Client:** Use `ADD JAR '/path/to/my-connector.jar';` at the SQL prompt

---

## 12. Complete Example

Below is a minimal but complete connector implementation.

### `MyConnectorOptions.java`

```java
package com.example.flink.connector.myconnector;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class MyConnectorOptions {

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hostname of the external system.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(9090)
                    .withDescription("Port of the external system.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table name in the external system.");

    private MyConnectorOptions() {}
}
```

### `MyConnectorFactory.java`

```java
package com.example.flink.connector.myconnector;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

public class MyConnectorFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "my-connector";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MyConnectorOptions.HOSTNAME);
        options.add(MyConnectorOptions.TABLE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MyConnectorOptions.PORT);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig opts = helper.getOptions();
        return new MyConnectorSource(
                opts.get(MyConnectorOptions.HOSTNAME),
                opts.get(MyConnectorOptions.PORT),
                opts.get(MyConnectorOptions.TABLE_NAME),
                context.getPhysicalRowDataType(),
                null);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig opts = helper.getOptions();
        return new MyConnectorSink(
                opts.get(MyConnectorOptions.HOSTNAME),
                opts.get(MyConnectorOptions.PORT),
                opts.get(MyConnectorOptions.TABLE_NAME),
                context.getPhysicalRowDataType(),
                1000,
                null);
    }
}
```

### `MyConnectorSource.java`

```java
package com.example.flink.connector.myconnector;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.types.DataType;

public class MyConnectorSource implements ScanTableSource {

    private final String hostname;
    private final int port;
    private final String tableName;
    private final DataType producedDataType;
    private final Integer parallelism;

    public MyConnectorSource(
            String hostname, int port, String tableName,
            DataType producedDataType, Integer parallelism) {
        this.hostname = hostname;
        this.port = port;
        this.tableName = tableName;
        this.producedDataType = producedDataType;
        this.parallelism = parallelism;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        // Replace with your actual Source<RowData, ?, ?> implementation
        // return SourceProvider.of(new MySource(hostname, port, tableName));
        throw new UnsupportedOperationException(
                "Replace with your Source implementation");
    }

    @Override
    public DynamicTableSource copy() {
        return new MyConnectorSource(
                hostname, port, tableName, producedDataType, parallelism);
    }

    @Override
    public String asSummaryString() {
        return "MyConnector";
    }
}
```

### `MyConnectorSink.java`

```java
package com.example.flink.connector.myconnector;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.types.DataType;

public class MyConnectorSink implements DynamicTableSink {

    private final String hostname;
    private final int port;
    private final String tableName;
    private final DataType consumedDataType;
    private final int bufferSize;
    private final Integer parallelism;

    public MyConnectorSink(
            String hostname, int port, String tableName,
            DataType consumedDataType, int bufferSize, Integer parallelism) {
        this.hostname = hostname;
        this.port = port;
        this.tableName = tableName;
        this.consumedDataType = consumedDataType;
        this.bufferSize = bufferSize;
        this.parallelism = parallelism;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        // Replace with your actual Sink<RowData> implementation
        // return SinkV2Provider.of(new MySink(hostname, port, tableName, bufferSize));
        throw new UnsupportedOperationException(
                "Replace with your Sink implementation");
    }

    @Override
    public DynamicTableSink copy() {
        return new MyConnectorSink(
                hostname, port, tableName, consumedDataType,
                bufferSize, parallelism);
    }

    @Override
    public String asSummaryString() {
        return "MyConnector";
    }
}
```

### `src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory`

```
com.example.flink.connector.myconnector.MyConnectorFactory
```

---

## 13. Reference

### Flink Type System — `RowData` Access

The table runtime operates on `RowData`, Flink's internal binary row format.
Access fields using typed getters:

```java
RowData row = ...;
long id       = row.getLong(0);
StringData s  = row.getString(1);   // StringData, not String
String name   = s.toString();
TimestampData ts = row.getTimestamp(2, 3);
boolean isNull = row.isNullAt(0);
```

Mapping from SQL types to `RowData` getters:

| SQL Type | RowData Getter | Java Type |
|---|---|---|
| `BOOLEAN` | `getBoolean(pos)` | `boolean` |
| `TINYINT` | `getByte(pos)` | `byte` |
| `SMALLINT` | `getShort(pos)` | `short` |
| `INT` | `getInt(pos)` | `int` |
| `BIGINT` | `getLong(pos)` | `long` |
| `FLOAT` | `getFloat(pos)` | `float` |
| `DOUBLE` | `getDouble(pos)` | `double` |
| `STRING` / `VARCHAR` | `getString(pos)` | `StringData` |
| `BYTES` / `VARBINARY` | `getBinary(pos)` | `byte[]` |
| `DECIMAL(p, s)` | `getDecimal(pos, p, s)` | `DecimalData` |
| `TIMESTAMP(p)` | `getTimestamp(pos, p)` | `TimestampData` |
| `DATE` | `getInt(pos)` | `int` (epoch days) |
| `TIME` | `getInt(pos)` | `int` (millis of day) |
| `ARRAY` | `getArray(pos)` | `ArrayData` |
| `MAP` | `getMap(pos)` | `MapData` |
| `ROW` | `getRow(pos, arity)` | `RowData` |

### Constructing `RowData` for Output

Use `GenericRowData` for producing rows:

```java
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;

GenericRowData row = new GenericRowData(RowKind.INSERT, 3);
row.setField(0, 42L);                               // BIGINT
row.setField(1, StringData.fromString("hello"));     // STRING
row.setField(2, TimestampData.fromEpochMillis(ts));  // TIMESTAMP
```

### `RowKind` — Change Data Capture

| RowKind | Meaning |
|---|---|
| `INSERT` (`+I`) | New row |
| `UPDATE_BEFORE` (`-U`) | Previous value before update |
| `UPDATE_AFTER` (`+U`) | New value after update |
| `DELETE` (`-D`) | Row deleted |

Access via `row.getRowKind()` and set via `row.setRowKind(RowKind.INSERT)`.

### Useful Built-in Connectors for Testing

| Connector | Type | Purpose |
|---|---|---|
| `datagen` | Source | Generate random/sequential test data |
| `print` | Sink | Print rows to stdout/stderr |
| `blackhole` | Sink | Discard all rows (performance testing) |
| `filesystem` | Source + Sink | Read/write files in various formats |
