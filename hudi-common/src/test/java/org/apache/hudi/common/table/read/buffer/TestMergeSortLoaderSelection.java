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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link DefaultFileGroupRecordBufferLoader#wouldSelectMergeSort}, the integration-level
 * decision the loader makes before opening any log file. Covers the RFC-81 fallback contract:
 * the merge-sort path must run only when the config flag is on AND every log block is ordered.
 * Any unordered, command, or corrupt block forces a fallback to the legacy spillable buffer.
 */
class TestMergeSortLoaderSelection {

  @Test
  void selectsMergeSortWhenConfigEnabledAndAllBlocksOrdered() {
    TypedProperties on = new TypedProperties();
    on.setProperty(DefaultFileGroupRecordBufferLoader.COMPACTION_WITH_MERGE_SORT_ENABLE_KEY, "true");
    assertTrue(DefaultFileGroupRecordBufferLoader.wouldSelectMergeSort(on,
        Arrays.asList(orderedDataBlock(), orderedDataBlock())));
  }

  @Test
  void fallsBackWhenConfigDisabled() {
    TypedProperties off = new TypedProperties();
    // Flag absent (default false) and explicitly false should both disable the merge-sort path.
    assertFalse(DefaultFileGroupRecordBufferLoader.wouldSelectMergeSort(off,
        Arrays.asList(orderedDataBlock(), orderedDataBlock())));
    off.setProperty(DefaultFileGroupRecordBufferLoader.COMPACTION_WITH_MERGE_SORT_ENABLE_KEY, "false");
    assertFalse(DefaultFileGroupRecordBufferLoader.wouldSelectMergeSort(off,
        Arrays.asList(orderedDataBlock(), orderedDataBlock())));
  }

  @Test
  void fallsBackWhenAnyBlockLacksIsOrdered() {
    TypedProperties on = new TypedProperties();
    on.setProperty(DefaultFileGroupRecordBufferLoader.COMPACTION_WITH_MERGE_SORT_ENABLE_KEY, "true");

    // A single unordered data block forces the legacy path even when others are ordered.
    assertFalse(DefaultFileGroupRecordBufferLoader.wouldSelectMergeSort(on,
        Arrays.asList(orderedDataBlock(), unorderedDataBlock())));

    // Per the issue's strict reading, command/corrupt blocks (which never carry IS_ORDERED)
    // also force a fallback even alongside ordered data blocks.
    assertFalse(DefaultFileGroupRecordBufferLoader.wouldSelectMergeSort(on,
        Arrays.asList(orderedDataBlock(), commandBlock())));
    assertFalse(DefaultFileGroupRecordBufferLoader.wouldSelectMergeSort(on,
        Arrays.asList(orderedDataBlock(), corruptBlock())));

    // Explicit IS_ORDERED=false also fails the check.
    assertFalse(DefaultFileGroupRecordBufferLoader.wouldSelectMergeSort(on,
        Arrays.asList(orderedDataBlock(), explicitlyUnorderedBlock())));
  }

  @Test
  void noLogBlocksIsTriviallyEligible() {
    // The loader has separate guards for empty log file lists; this helper just checks the
    // header invariant. With no blocks to inspect, the helper trivially returns true when the
    // flag is on.
    TypedProperties on = new TypedProperties();
    on.setProperty(DefaultFileGroupRecordBufferLoader.COMPACTION_WITH_MERGE_SORT_ENABLE_KEY, "true");
    assertTrue(DefaultFileGroupRecordBufferLoader.wouldSelectMergeSort(on,
        Collections.<HoodieLogBlock>emptyList()));
  }

  // ---------------------------------------------------------------------------
  // Stub blocks
  // ---------------------------------------------------------------------------

  private static HoodieLogBlock orderedDataBlock() {
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.IS_ORDERED, "true");
    return new StubBlock(HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK, header);
  }

  private static HoodieLogBlock unorderedDataBlock() {
    return new StubBlock(HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK, Collections.emptyMap());
  }

  private static HoodieLogBlock explicitlyUnorderedBlock() {
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.IS_ORDERED, "false");
    return new StubBlock(HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK, header);
  }

  private static HoodieLogBlock commandBlock() {
    return new StubBlock(HoodieLogBlock.HoodieLogBlockType.COMMAND_BLOCK, Collections.emptyMap());
  }

  private static HoodieLogBlock corruptBlock() {
    return new StubBlock(HoodieLogBlock.HoodieLogBlockType.CORRUPT_BLOCK, Collections.emptyMap());
  }

  private static final class StubBlock extends HoodieLogBlock {
    private final HoodieLogBlockType type;

    StubBlock(HoodieLogBlockType type, Map<HeaderMetadataType, String> header) {
      super(header, Collections.emptyMap(), Option.empty(), Option.empty(), null, false);
      this.type = type;
    }

    @Override
    public HoodieLogBlockType getBlockType() {
      return type;
    }
  }
}
