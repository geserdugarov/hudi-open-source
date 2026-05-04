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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Streaming merge-sort log record scanner used by RFC-81's ordered-data compaction path.
 * <p>
 * Each input is treated as an independent, key-sorted stream:
 * <ul>
 *   <li>One stream per ordered {@link HoodieLogBlock} in the file group.</li>
 *   <li>One stream for the (already-sorted) base file, when present.</li>
 * </ul>
 * Streams are merged via a min-heap ordered by the record key of each stream's buffered head.
 * When two or more streams expose the same key the records are folded together using the supplied
 * merge function (which production callers wire to {@link HoodieRecordMerger}). Streams earlier in
 * the input list are treated as <em>older</em>; later ones as <em>newer</em>.
 * <p>
 * Per-stream buffers are refilled when emptied, sized by
 * {@code hoodie.compaction.log.streaming.read.buffer} (default 10MB). An exhausted stream is
 * removed from the heap.
 * <p>
 * Crucially, this scanner does <strong>not</strong> allocate an
 * {@link org.apache.hudi.common.util.collection.ExternalSpillableMap} or any other on-disk spill
 * structure — the entire merge runs in heap-bounded streaming memory. Callers must fall back to
 * {@link HoodieMergedLogRecordScanner} when the prerequisites for streaming merge do not hold;
 * see {@link #shouldUseMergeSort(boolean, Iterable)}.
 */
@NotThreadSafe
public class HoodieMergeSortLogRecordScanner<T> implements Iterable<HoodieRecord<T>>, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMergeSortLogRecordScanner.class);

  /** Default per-stream buffer size, mirroring {@code hoodie.compaction.log.streaming.read.buffer}. */
  public static final long DEFAULT_PER_STREAM_BUFFER_BYTES = 10L * 1024 * 1024;

  private final List<RecordStream<T>> streams;
  private final RecordMergeFn<T> mergeFn;
  private boolean iteratorReturned = false;
  private boolean closed = false;

  private HoodieMergeSortLogRecordScanner(List<RecordStream<T>> streams, RecordMergeFn<T> mergeFn) {
    this.streams = streams;
    this.mergeFn = mergeFn;
  }

  /**
   * Function invoked whenever two records share the same key. Streams earlier in the input list
   * are passed as {@code older}; later ones as {@code newer}. Implementations must return the
   * record that should be emitted (or a fresh, merged instance).
   */
  @FunctionalInterface
  public interface RecordMergeFn<T> {
    HoodieRecord<T> merge(HoodieRecord<T> older, HoodieRecord<T> newer) throws IOException;
  }

  @Override
  public ClosableIterator<HoodieRecord<T>> iterator() {
    checkArgument(!iteratorReturned, "iterator() may only be invoked once on a HoodieMergeSortLogRecordScanner");
    iteratorReturned = true;
    return new MergeSortIterator();
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    IOException firstFailure = null;
    for (RecordStream<T> s : streams) {
      try {
        s.close();
      } catch (IOException e) {
        if (firstFailure == null) {
          firstFailure = e;
        } else {
          firstFailure.addSuppressed(e);
        }
      }
    }
    if (firstFailure != null) {
      throw new HoodieIOException("Failed to close one or more merge-sort scanner streams", firstFailure);
    }
  }

  /**
   * Returns {@code true} when streaming merge-sort is applicable to the supplied blocks. The path
   * is enabled only when:
   * <ul>
   *   <li>{@code hoodie.compaction.with.merge.sort.enable} is set, and</li>
   *   <li>every data/delete block in the file group has the {@code IS_ORDERED} header set.</li>
   * </ul>
   * Non-data blocks (e.g. command/corrupt blocks) are ignored when evaluating ordering.
   */
  public static boolean shouldUseMergeSort(boolean configEnabled, Iterable<HoodieLogBlock> blocks) {
    if (!configEnabled) {
      return false;
    }
    for (HoodieLogBlock block : blocks) {
      if (!block.isDataOrDeleteBlock()) {
        continue;
      }
      if (!block.isOrdered()) {
        return false;
      }
    }
    return true;
  }

  // ---------------------------------------------------------------------------
  // Internal: per-stream buffer + heap-driven iteration
  // ---------------------------------------------------------------------------

  /**
   * Wraps a single key-sorted input source. Owns a record buffer that is refilled from
   * {@link #source} whenever it drains, until the underlying iterator reports EOF.
   */
  private static final class RecordStream<T> implements Closeable {
    private final String name;
    /** Older streams come first; this index is used to break ties in the heap and feed the merger. */
    private final int orderIndex;
    private final ClosableIterator<HoodieRecord<T>> source;
    private final Deque<HoodieRecord<T>> buffer = new ArrayDeque<>();
    private final long bufferBytes;
    private final SizeEstimator<HoodieRecord<T>> sizeEstimator;
    private boolean exhausted;

    RecordStream(String name,
                 int orderIndex,
                 ClosableIterator<HoodieRecord<T>> source,
                 long bufferBytes,
                 SizeEstimator<HoodieRecord<T>> sizeEstimator) {
      this.name = name;
      this.orderIndex = orderIndex;
      this.source = source;
      this.bufferBytes = bufferBytes;
      this.sizeEstimator = sizeEstimator;
    }

    /** Ensure the buffer has at least one record; returns {@code true} when a head is available. */
    boolean prime() {
      if (!buffer.isEmpty()) {
        return true;
      }
      if (exhausted) {
        return false;
      }
      refill();
      return !buffer.isEmpty();
    }

    HoodieRecord<T> peek() {
      return buffer.peek();
    }

    HoodieRecord<T> pop() {
      HoodieRecord<T> head = buffer.pollFirst();
      if (head == null) {
        throw new NoSuchElementException("RecordStream " + name + " has no buffered head");
      }
      return head;
    }

    private void refill() {
      long sizeAccum = 0L;
      while (sizeAccum < bufferBytes && source.hasNext()) {
        HoodieRecord<T> next = source.next();
        buffer.addLast(next);
        sizeAccum += Math.max(1L, sizeEstimator.sizeEstimate(next));
      }
      if (!source.hasNext()) {
        exhausted = true;
      }
    }

    @Override
    public void close() throws IOException {
      buffer.clear();
      source.close();
    }
  }

  private final class MergeSortIterator implements ClosableIterator<HoodieRecord<T>> {

    private final PriorityQueue<RecordStream<T>> heap;
    private boolean iteratorClosed = false;

    MergeSortIterator() {
      // Order primarily by the buffered head's record key; break ties by orderIndex (older first)
      // so that the merger receives streams in deterministic age order.
      Comparator<RecordStream<T>> cmp = Comparator
          .comparing((RecordStream<T> s) -> s.peek().getRecordKey())
          .thenComparingInt(s -> s.orderIndex);
      this.heap = new PriorityQueue<>(Math.max(1, streams.size()), cmp);
      for (RecordStream<T> stream : streams) {
        if (stream.prime()) {
          heap.offer(stream);
        }
      }
    }

    @Override
    public boolean hasNext() {
      return !iteratorClosed && !heap.isEmpty();
    }

    @Override
    public HoodieRecord<T> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      RecordStream<T> top = heap.poll();
      String key = top.peek().getRecordKey();

      // Collect all streams whose current head shares this key. We pop them off the heap so we
      // can advance and re-insert them after merging in deterministic age order.
      List<RecordStream<T>> sameKey = new ArrayList<>();
      sameKey.add(top);
      while (!heap.isEmpty() && heap.peek().peek().getRecordKey().equals(key)) {
        sameKey.add(heap.poll());
      }

      // Pop one record from each stream sharing the key, in older→newer order.
      sameKey.sort(Comparator.comparingInt(s -> s.orderIndex));
      HoodieRecord<T> merged = sameKey.get(0).pop();
      try {
        for (int i = 1; i < sameKey.size(); i++) {
          HoodieRecord<T> next = sameKey.get(i).pop();
          merged = mergeFn.merge(merged, next);
        }
      } catch (IOException e) {
        throw new HoodieIOException("Failed to merge records sharing key " + key, e);
      }

      // Re-insert any stream that still has more buffered/unread records.
      for (RecordStream<T> s : sameKey) {
        if (s.prime()) {
          heap.offer(s);
        }
      }

      return merged;
    }

    @Override
    public void close() {
      if (iteratorClosed) {
        return;
      }
      iteratorClosed = true;
      heap.clear();
      HoodieMergeSortLogRecordScanner.this.close();
    }
  }

  // ---------------------------------------------------------------------------
  // Builder
  // ---------------------------------------------------------------------------

  public static <T> Builder<T> newBuilder() {
    return new Builder<>();
  }

  /**
   * Builder for {@link HoodieMergeSortLogRecordScanner}. Streams must be supplied via
   * {@link #addStream} in oldest-to-newest order (the base file, when present, comes first).
   * The merge function can be set explicitly with {@link #withMergeFn}, or assembled from a
   * {@link HoodieRecordMerger} via {@link #withRecordMerger}.
   */
  public static class Builder<T> {
    private final List<RecordStream<T>> streams = new ArrayList<>();
    private long bufferBytes = DEFAULT_PER_STREAM_BUFFER_BYTES;
    private SizeEstimator<HoodieRecord<T>> sizeEstimator = record -> 1L;
    private RecordMergeFn<T> mergeFn;

    public Builder<T> addStream(String name, ClosableIterator<HoodieRecord<T>> source) {
      streams.add(new RecordStream<>(name, streams.size(), source, bufferBytes, sizeEstimator));
      return this;
    }

    public Builder<T> withBufferBytes(long bufferBytes) {
      checkArgument(bufferBytes > 0, "bufferBytes must be positive");
      this.bufferBytes = bufferBytes;
      return this;
    }

    public Builder<T> withSizeEstimator(SizeEstimator<HoodieRecord<T>> sizeEstimator) {
      this.sizeEstimator = sizeEstimator;
      return this;
    }

    public Builder<T> withMergeFn(RecordMergeFn<T> mergeFn) {
      this.mergeFn = mergeFn;
      return this;
    }

    /**
     * Wires the supplied {@link HoodieRecordMerger} as the merge function, marshaling records
     * through {@link BufferedRecord} the same way {@link HoodieMergedLogRecordScanner} does so the
     * streaming path produces identical merge semantics to the legacy spill-map path.
     */
    public Builder<T> withRecordMerger(HoodieRecordMerger merger,
                                       RecordContext<T> recordContext,
                                       HoodieSchema readerSchema,
                                       TypedProperties payloadProps,
                                       String[] orderingFields,
                                       DeleteContext deleteContext) {
      this.mergeFn = (older, newer) -> {
        BufferedRecord<T> olderBuf = BufferedRecords.fromHoodieRecord(
            older, readerSchema, recordContext, payloadProps, orderingFields, deleteContext);
        BufferedRecord<T> newerBuf = BufferedRecords.fromHoodieRecord(
            newer, readerSchema, recordContext, payloadProps, orderingFields, deleteContext);
        BufferedRecord<T> combined = merger.merge(olderBuf, newerBuf, recordContext, payloadProps);
        // Pre-combine often returns the older or newer instance unchanged; preserve the original
        // HoodieRecord identity in those cases so downstream pipelines don't pay reconstruction
        // cost on the common path.
        if (combined.getRecord() == older.getData()) {
          return older;
        }
        if (combined.getRecord() == newer.getData()) {
          return newer;
        }
        return recordContext.constructFinalHoodieRecord(combined);
      };
      return this;
    }

    public HoodieMergeSortLogRecordScanner<T> build() {
      checkArgument(mergeFn != null, "Either a merge function or a record merger must be supplied");
      LOG.info("Building HoodieMergeSortLogRecordScanner with {} input stream(s) and per-stream buffer of {} bytes",
          streams.size(), bufferBytes);
      return new HoodieMergeSortLogRecordScanner<>(new ArrayList<>(streams), mergeFn);
    }
  }
}
