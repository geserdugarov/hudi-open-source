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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;

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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link HoodieMergeSortLogRecordScanner}.
 */
class TestHoodieMergeSortLogRecordScanner {

  private static final String SCHEMA_STR = "{\"type\":\"record\",\"name\":\"Test\",\"fields\":["
      + "{\"name\":\"id\",\"type\":\"string\"},"
      + "{\"name\":\"value\",\"type\":\"string\"},"
      + "{\"name\":\"version\",\"type\":\"long\"}]}";
  private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STR);
  private static final int VALUE_POS = SCHEMA.getField("value").pos();

  /**
   * Synthetic file group: one sorted "base file" stream and two sorted log block streams.
   * Verifies that overlapping keys fold via the merger (last-write-wins here) and that the
   * output is globally key-sorted.
   */
  @Test
  void mergesSortedBaseFileAndLogBlocksProducingKeySortedOutput() {
    // Base file (oldest). Two records that will be overwritten in the log blocks.
    List<HoodieRecord<IndexedRecord>> base = Arrays.asList(
        record("k1", "base", 1L),
        record("k2", "base", 1L),
        record("k4", "base-only", 1L));

    // Log block #1 (older log block). Updates k1 and adds k3.
    List<HoodieRecord<IndexedRecord>> block1 = Arrays.asList(
        record("k1", "log1", 2L),
        record("k3", "log1", 2L));

    // Log block #2 (newest). Updates k1 again and adds k5.
    List<HoodieRecord<IndexedRecord>> block2 = Arrays.asList(
        record("k1", "log2", 3L),
        record("k2", "log2", 3L),
        record("k5", "log2", 3L));

    AtomicInteger mergeCalls = new AtomicInteger(0);
    HoodieMergeSortLogRecordScanner.RecordMergeFn<IndexedRecord> lastWriteWins = (older, newer) -> {
      mergeCalls.incrementAndGet();
      // Both records survived into the merger; choosing newer matches commit-time semantics.
      return newer;
    };

    try (HoodieMergeSortLogRecordScanner<IndexedRecord> scanner =
             HoodieMergeSortLogRecordScanner.<IndexedRecord>newBuilder()
                 .addStream("base", ClosableIterator.wrap(base.iterator()))
                 .addStream("log1", ClosableIterator.wrap(block1.iterator()))
                 .addStream("log2", ClosableIterator.wrap(block2.iterator()))
                 .withMergeFn(lastWriteWins)
                 .withBufferBytes(1024)
                 .build()) {

      List<HoodieRecord<IndexedRecord>> emitted = new ArrayList<>();
      try (ClosableIterator<HoodieRecord<IndexedRecord>> it = scanner.iterator()) {
        while (it.hasNext()) {
          emitted.add(it.next());
        }
      }

      // Output must be sorted by key.
      List<String> keys = new ArrayList<>();
      for (HoodieRecord<IndexedRecord> r : emitted) {
        keys.add(r.getRecordKey());
      }
      assertEquals(Arrays.asList("k1", "k2", "k3", "k4", "k5"), keys);

      // k1 was present in base, log1, log2 → 2 merge calls.
      // k2 was present in base, log2 → 1 merge call.
      // k3, k4, k5 are unique → no merge call each.
      assertEquals(3, mergeCalls.get(),
          "Merger should be invoked once per overlapping pair");

      Map<String, String> valueByKey = new HashMap<>();
      for (HoodieRecord<IndexedRecord> r : emitted) {
        valueByKey.put(r.getRecordKey(), valueOf(r));
      }
      assertEquals("log2", valueByKey.get("k1"), "k1 should be the newest value (log2)");
      assertEquals("log2", valueByKey.get("k2"), "k2 should be the newest value (log2)");
      assertEquals("log1", valueByKey.get("k3"));
      assertEquals("base-only", valueByKey.get("k4"));
      assertEquals("log2", valueByKey.get("k5"));
    }
  }

  /**
   * The empty-stream case must not break heap initialization, and a single-stream input should
   * pass through unchanged.
   */
  @Test
  void handlesEmptyAndSingleStreamInputs() {
    HoodieMergeSortLogRecordScanner.RecordMergeFn<IndexedRecord> nopMerger = (older, newer) -> newer;

    try (HoodieMergeSortLogRecordScanner<IndexedRecord> empty =
             HoodieMergeSortLogRecordScanner.<IndexedRecord>newBuilder()
                 .addStream("empty", ClosableIterator.wrap(Collections.<HoodieRecord<IndexedRecord>>emptyIterator()))
                 .withMergeFn(nopMerger)
                 .build()) {
      try (ClosableIterator<HoodieRecord<IndexedRecord>> it = empty.iterator()) {
        assertFalse(it.hasNext());
      }
    }

    List<HoodieRecord<IndexedRecord>> single = Arrays.asList(
        record("a", "v", 1L), record("b", "v", 1L), record("c", "v", 1L));
    try (HoodieMergeSortLogRecordScanner<IndexedRecord> scanner =
             HoodieMergeSortLogRecordScanner.<IndexedRecord>newBuilder()
                 .addStream("only", ClosableIterator.wrap(single.iterator()))
                 .withMergeFn(nopMerger)
                 .build()) {
      List<String> keys = new ArrayList<>();
      try (ClosableIterator<HoodieRecord<IndexedRecord>> it = scanner.iterator()) {
        while (it.hasNext()) {
          keys.add(it.next().getRecordKey());
        }
      }
      assertEquals(Arrays.asList("a", "b", "c"), keys);
    }
  }

  @Test
  void iteratorIsSingleUseToAvoidAccidentalDoubleConsumption() {
    HoodieMergeSortLogRecordScanner<IndexedRecord> scanner =
        HoodieMergeSortLogRecordScanner.<IndexedRecord>newBuilder()
            .addStream("only", ClosableIterator.wrap(Arrays.asList(record("a", "v", 1L)).iterator()))
            .withMergeFn((older, newer) -> newer)
            .build();
    try (ClosableIterator<HoodieRecord<IndexedRecord>> first = scanner.iterator()) {
      assertTrue(first.hasNext());
    }
    assertThrows(IllegalArgumentException.class, scanner::iterator);
  }

  /**
   * Streaming merge-sort is opt-in: it must run only when the config is enabled <em>and</em> every
   * data/delete block carries IS_ORDERED. Any unordered block forces a fallback. Non-data blocks
   * (command, corrupt) do not participate in the decision.
   */
  @Test
  void shouldUseMergeSortRespectsConfigAndOrderingFlag() {
    HoodieLogBlock orderedDataBlock = stubDataBlock(true);
    HoodieLogBlock orderedDeleteBlock = stubDeleteBlock(true);
    HoodieLogBlock unorderedDataBlock = stubDataBlock(false);
    HoodieLogBlock commandBlock = stubCommandBlock();
    HoodieLogBlock corruptBlock = stubCorruptBlock();

    // Disabled config → never use merge-sort.
    assertFalse(HoodieMergeSortLogRecordScanner.shouldUseMergeSort(
        false, Arrays.asList(orderedDataBlock, orderedDeleteBlock)));

    // All blocks ordered → enabled.
    assertTrue(HoodieMergeSortLogRecordScanner.shouldUseMergeSort(
        true, Arrays.asList(orderedDataBlock, orderedDeleteBlock)));

    // Any unordered data block forces fallback.
    assertFalse(HoodieMergeSortLogRecordScanner.shouldUseMergeSort(
        true, Arrays.asList(orderedDataBlock, unorderedDataBlock, orderedDeleteBlock)));

    // Command and corrupt blocks are ignored when evaluating ordering.
    assertTrue(HoodieMergeSortLogRecordScanner.shouldUseMergeSort(
        true, Arrays.asList(commandBlock, orderedDataBlock, corruptBlock)));

    // No data blocks at all → vacuously enabled (the merge-sort path will simply emit nothing).
    assertTrue(HoodieMergeSortLogRecordScanner.shouldUseMergeSort(
        true, Collections.singletonList(commandBlock)));
  }

  /**
   * Guards against accidental regression where a future contributor wires the scanner through an
   * ExternalSpillableMap. The merge-sort path is bounded-memory by design and must never hold a
   * field reference to a spillable map.
   */
  @Test
  void mergeSortScannerHoldsNoSpillableMapField() {
    Class<?> scannerClass = HoodieMergeSortLogRecordScanner.class;
    for (Field field : scannerClass.getDeclaredFields()) {
      assertFalse(ExternalSpillableMap.class.isAssignableFrom(field.getType()),
          "HoodieMergeSortLogRecordScanner must not declare an ExternalSpillableMap field, "
              + "but found one named '" + field.getName() + "'");
    }
    // Also check inner classes.
    for (Class<?> inner : scannerClass.getDeclaredClasses()) {
      for (Field field : inner.getDeclaredFields()) {
        assertFalse(ExternalSpillableMap.class.isAssignableFrom(field.getType()),
            "Inner class " + inner.getSimpleName() + " must not declare an ExternalSpillableMap field, "
                + "but found one named '" + field.getName() + "'");
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static HoodieRecord<IndexedRecord> record(String key, String value, long version) {
    GenericRecord avro = new GenericData.Record(SCHEMA);
    avro.put("id", key);
    avro.put("value", value);
    avro.put("version", version);
    HoodieAvroIndexedRecord rec = new HoodieAvroIndexedRecord(new HoodieKey(key, "p"), avro, (Comparable<?>) version);
    @SuppressWarnings({"unchecked", "rawtypes"})
    HoodieRecord<IndexedRecord> typed = (HoodieRecord) rec;
    return typed;
  }

  private static String valueOf(HoodieRecord<IndexedRecord> record) {
    return String.valueOf(record.getData().get(VALUE_POS));
  }

  private static HoodieLogBlock stubDataBlock(boolean ordered) {
    return new TestLogBlock(HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK, ordered);
  }

  private static HoodieLogBlock stubDeleteBlock(boolean ordered) {
    return new TestLogBlock(HoodieLogBlock.HoodieLogBlockType.DELETE_BLOCK, ordered);
  }

  private static HoodieLogBlock stubCommandBlock() {
    return new TestLogBlock(HoodieLogBlock.HoodieLogBlockType.COMMAND_BLOCK, false);
  }

  private static HoodieLogBlock stubCorruptBlock() {
    return new TestLogBlock(HoodieLogBlock.HoodieLogBlockType.CORRUPT_BLOCK, false);
  }

  /** Minimal {@link HoodieLogBlock} stub exposing only the type and IS_ORDERED header. */
  private static final class TestLogBlock extends HoodieLogBlock {
    private final HoodieLogBlockType type;

    private TestLogBlock(HoodieLogBlockType type, boolean ordered) {
      super(buildHeader(ordered), Collections.emptyMap(), Option.empty(), Option.empty(), null, false);
      this.type = type;
    }

    private static Map<HeaderMetadataType, String> buildHeader(boolean ordered) {
      Map<HeaderMetadataType, String> header = new HashMap<>();
      if (ordered) {
        header.put(HeaderMetadataType.IS_ORDERED, "true");
      }
      return header;
    }

    @Override
    public HoodieLogBlockType getBlockType() {
      return type;
    }
  }
}
