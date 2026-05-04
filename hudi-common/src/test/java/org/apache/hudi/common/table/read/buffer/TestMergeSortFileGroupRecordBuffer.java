/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the streaming, spill-free {@link MergeSortFileGroupRecordBuffer}.
 */
class TestMergeSortFileGroupRecordBuffer {

  private static final String SCHEMA_STR = "{\"type\":\"record\",\"name\":\"Test\",\"fields\":["
      + "{\"name\":\"record_key\",\"type\":\"string\"},"
      + "{\"name\":\"value\",\"type\":\"string\"},"
      + "{\"name\":\"ts\",\"type\":\"long\"}]}";
  private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(SCHEMA_STR);
  private static final HoodieSchema SCHEMA = HoodieSchema.fromAvroSchema(AVRO_SCHEMA);
  private static final int VALUE_POS = AVRO_SCHEMA.getField("value").pos();

  /**
   * Direct invariant: {@link MergeSortFileGroupRecordBuffer} must not declare or transitively
   * hold an {@link ExternalSpillableMap} field. This is the core RFC-81 promise that the
   * production merge-sort path runs in heap-bounded streaming memory.
   */
  @Test
  void bufferDoesNotAllocateOrHoldASpillableMap() {
    Class<?> bufferClass = MergeSortFileGroupRecordBuffer.class;
    for (Field field : bufferClass.getDeclaredFields()) {
      assertFalse(ExternalSpillableMap.class.isAssignableFrom(field.getType()),
          "MergeSortFileGroupRecordBuffer must not declare an ExternalSpillableMap field, "
              + "found '" + field.getName() + "'");
    }
    // Crucially, the merge-sort buffer must not extend FileGroupRecordBuffer either, which would
    // pull in the spillable map allocation through the parent constructor.
    assertFalse(FileGroupRecordBuffer.class.isAssignableFrom(bufferClass),
        "MergeSortFileGroupRecordBuffer must not extend FileGroupRecordBuffer, otherwise the parent "
            + "constructor would allocate an ExternalSpillableMap");
  }

  /**
   * End-to-end: feed the buffer two ordered, in-memory data blocks plus a sorted base file
   * iterator and verify the streaming merge-sort produces correctly key-sorted output.
   * <p>
   * No real log files, no spillable map, no legacy reader.
   */
  @Test
  void streamsMergedSortedOutputAcrossBaseFileAndOrderedLogBlocks() throws Exception {
    // Base file (oldest stream) — sorted by record_key.
    List<IndexedRecord> baseRecords = Arrays.asList(
        avro("k1", "base", 1L),
        avro("k2", "base", 1L),
        avro("k4", "base-only", 1L));

    // First ordered log block — overlaps k1 (newer than base), introduces k3.
    HoodieDataBlock block1 = orderedAvroBlock(Arrays.asList(
        hoodieRecord("k1", "log1", 2L),
        hoodieRecord("k3", "log1", 2L)));

    // Second ordered log block — newest. Overlaps k1, k2; introduces k5.
    HoodieDataBlock block2 = orderedAvroBlock(Arrays.asList(
        hoodieRecord("k1", "log2", 3L),
        hoodieRecord("k2", "log2", 3L),
        hoodieRecord("k5", "log2", 3L)));

    HoodieReaderContext<IndexedRecord> readerContext = newReaderContext();
    DeleteContext deleteContext = readerContext.getSchemaHandler().getDeleteContext().withReaderSchema(SCHEMA);

    MergeSortFileGroupRecordBuffer<IndexedRecord> buffer = new MergeSortFileGroupRecordBuffer<>(
        readerContext,
        Arrays.asList(block1, block2),
        Arrays.asList("100", "200"),
        readerContext.getRecordMerger().get(),
        SCHEMA,
        new String[] {"ts"},
        deleteContext,
        UpdateProcessor.create(new HoodieReadStats(), readerContext, false, Option.empty(), new TypedProperties()),
        new TypedProperties(),
        1024L);

    buffer.setBaseFileIterator(ClosableIterator.wrap(baseRecords.iterator()));

    List<IndexedRecord> emitted = new ArrayList<>();
    while (buffer.hasNext()) {
      BufferedRecord<IndexedRecord> next = buffer.next();
      emitted.add(next.getRecord());
    }
    buffer.close();

    List<String> emittedKeys = new ArrayList<>();
    Map<String, String> valueByKey = new HashMap<>();
    for (IndexedRecord r : emitted) {
      String key = r.get(0).toString();
      emittedKeys.add(key);
      valueByKey.put(key, r.get(VALUE_POS).toString());
    }
    assertEquals(Arrays.asList("k1", "k2", "k3", "k4", "k5"), emittedKeys,
        "Output must be key-sorted across base file and all ordered log blocks");
    // With CommitTimeOrdering merge mode, the newest record (latest stream) wins.
    assertEquals("log2", valueByKey.get("k1"));
    assertEquals("log2", valueByKey.get("k2"));
    assertEquals("log1", valueByKey.get("k3"));
    assertEquals("base-only", valueByKey.get("k4"));
    assertEquals("log2", valueByKey.get("k5"));
  }

  /**
   * The buffer also handles the no-base-file case (file group with log files only): it passes
   * the merged log records straight through.
   */
  @Test
  void streamsLogBlocksOnlyWhenNoBaseFile() throws Exception {
    HoodieDataBlock block1 = orderedAvroBlock(Arrays.asList(
        hoodieRecord("k1", "log1", 1L),
        hoodieRecord("k2", "log1", 1L)));
    HoodieDataBlock block2 = orderedAvroBlock(Collections.singletonList(
        hoodieRecord("k1", "log2", 2L)));

    HoodieReaderContext<IndexedRecord> readerContext = newReaderContext();
    DeleteContext deleteContext = readerContext.getSchemaHandler().getDeleteContext().withReaderSchema(SCHEMA);

    MergeSortFileGroupRecordBuffer<IndexedRecord> buffer = new MergeSortFileGroupRecordBuffer<>(
        readerContext,
        Arrays.asList(block1, block2),
        Arrays.asList("100", "200"),
        readerContext.getRecordMerger().get(),
        SCHEMA,
        new String[] {"ts"},
        deleteContext,
        UpdateProcessor.create(new HoodieReadStats(), readerContext, false, Option.empty(), new TypedProperties()),
        new TypedProperties(),
        1024L);

    // No setBaseFileIterator — emulating a file group with no base file.
    List<String> keys = new ArrayList<>();
    while (buffer.hasNext()) {
      keys.add(buffer.next().getRecord().get(0).toString());
    }
    buffer.close();
    assertEquals(Arrays.asList("k1", "k2"), keys);
  }

  /**
   * Process-* entry points are the legacy-reader hook surface; the merge-sort path bypasses
   * the legacy reader entirely. Calling them must surface as a clear error so accidental
   * routing through the spill-map flow is caught immediately.
   */
  @Test
  void legacyProcessEntryPointsAreRejected() {
    HoodieReaderContext<IndexedRecord> readerContext = newReaderContext();
    DeleteContext deleteContext = readerContext.getSchemaHandler().getDeleteContext().withReaderSchema(SCHEMA);
    MergeSortFileGroupRecordBuffer<IndexedRecord> buffer = new MergeSortFileGroupRecordBuffer<>(
        readerContext, Collections.emptyList(), Collections.emptyList(),
        readerContext.getRecordMerger().get(), SCHEMA, new String[] {"ts"},
        deleteContext,
        UpdateProcessor.create(new HoodieReadStats(), readerContext, false, Option.empty(), new TypedProperties()),
        new TypedProperties(), 1024L);
    org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
        () -> buffer.processDataBlock(orderedAvroBlock(Collections.emptyList()), Option.empty()));
    org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
        () -> buffer.processNextDataRecord(null, "k"));
    org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
        () -> buffer.processDeleteBlock(null));
    org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
        () -> buffer.processNextDeletedRecord(null, "k"));
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static IndexedRecord avro(String key, String value, long ts) {
    GenericRecord rec = new GenericData.Record(AVRO_SCHEMA);
    rec.put("record_key", key);
    rec.put("value", value);
    rec.put("ts", ts);
    return rec;
  }

  private static HoodieRecord hoodieRecord(String key, String value, long ts) {
    return new HoodieAvroIndexedRecord(new HoodieKey(key, ""), avro(key, value, ts), (Comparable<?>) ts);
  }

  /** Builds an in-memory {@link HoodieAvroDataBlock} carrying {@code IS_ORDERED=true}. */
  private static HoodieDataBlock orderedAvroBlock(List<HoodieRecord> records) {
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, SCHEMA_STR);
    header.put(HoodieLogBlock.HeaderMetadataType.IS_ORDERED, "true");
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, String.valueOf(System.nanoTime()));
    return new HoodieAvroDataBlock(records, header, "record_key");
  }

  private static HoodieReaderContext<IndexedRecord> newReaderContext() {
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.COMMIT_TIME_ORDERING);
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.NINE);
    when(tableConfig.getPartialUpdateMode()).thenReturn(Option.empty());
    when(tableConfig.getOrderingFieldsStr()).thenReturn(Option.empty());
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[] {"record_key"}));

    @SuppressWarnings("unchecked")
    StorageConfiguration<Object> storageConfiguration = (StorageConfiguration<Object>) mock(StorageConfiguration.class);
    HoodieReaderContext<IndexedRecord> readerContext =
        new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    readerContext.initRecordMerger(new TypedProperties());

    @SuppressWarnings("unchecked")
    FileGroupReaderSchemaHandler<IndexedRecord> schemaHandler = mock(FileGroupReaderSchemaHandler.class);
    when(schemaHandler.getRequiredSchema()).thenReturn(SCHEMA);
    when(schemaHandler.getRequestedSchema()).thenReturn(SCHEMA);
    when(schemaHandler.getSchemaForUpdates()).thenReturn(SCHEMA);
    when(schemaHandler.getInternalSchema()).thenReturn(InternalSchema.getEmptyInternalSchema());
    when(schemaHandler.getDeleteContext()).thenReturn(new DeleteContext(new TypedProperties(), SCHEMA));
    readerContext.setSchemaHandler(schemaHandler);
    return readerContext;
  }
}
