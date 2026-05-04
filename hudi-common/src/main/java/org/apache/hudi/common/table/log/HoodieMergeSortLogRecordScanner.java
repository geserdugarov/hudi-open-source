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
import org.apache.hudi.common.util.ObjectSizeCalculator;
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
import java.util.function.Function;

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
 * When two or more streams expose the same key — or a single stream emits adjacent records with
 * the same key — the records are folded together using the supplied merge function (which
 * production callers wire to {@link HoodieRecordMerger}). Streams earlier in the input list are
 * treated as <em>older</em>; later ones as <em>newer</em>. Within a single stream, records
 * encountered earlier are older than records encountered later.
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
 *
 * @param <R> the in-stream record representation (e.g. {@link HoodieRecord}, or
 *            {@link BufferedRecord} on the production read path).
 */
@NotThreadSafe
public class HoodieMergeSortLogRecordScanner<R> implements Iterable<R>, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMergeSortLogRecordScanner.class);

  /** Default per-stream buffer size, mirroring {@code hoodie.compaction.log.streaming.read.buffer}. */
  public static final long DEFAULT_PER_STREAM_BUFFER_BYTES = 10L * 1024 * 1024;

  private final List<RecordStream<R>> streams;
  private final Function<R, String> keyExtractor;
  private final RecordMergeFn<R> mergeFn;
  private boolean iteratorReturned = false;
  private boolean closed = false;

  private HoodieMergeSortLogRecordScanner(List<RecordStream<R>> streams,
                                          Function<R, String> keyExtractor,
                                          RecordMergeFn<R> mergeFn) {
    this.streams = streams;
    this.keyExtractor = keyExtractor;
    this.mergeFn = mergeFn;
  }

  /**
   * Function invoked whenever two records share the same key. Streams earlier in the input list
   * are passed as {@code older}; later ones as {@code newer}. Implementations must return the
   * record that should be emitted (or a fresh, merged instance).
   */
  @FunctionalInterface
  public interface RecordMergeFn<R> {
    R merge(R older, R newer) throws IOException;
  }

  @Override
  public ClosableIterator<R> iterator() {
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
    for (RecordStream<R> s : streams) {
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
   *   <li><strong>every</strong> log block in the file group has the {@code IS_ORDERED} header
   *       set to {@code true}.</li>
   * </ul>
   * Per RFC-81, presence of any block lacking {@code IS_ORDERED} — including command, corrupt,
   * and legacy unordered data blocks — forces a fallback to
   * {@link HoodieMergedLogRecordScanner}.
   */
  public static boolean shouldUseMergeSort(boolean configEnabled, Iterable<HoodieLogBlock> blocks) {
    if (!configEnabled) {
      return false;
    }
    for (HoodieLogBlock block : blocks) {
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
  private static final class RecordStream<R> implements Closeable {
    private final String name;
    /** Older streams come first; this index is used to break ties in the heap and feed the merger. */
    private final int orderIndex;
    private final ClosableIterator<R> source;
    private final Deque<R> buffer = new ArrayDeque<>();
    private final long bufferBytes;
    private final SizeEstimator<R> sizeEstimator;
    private boolean exhausted;

    RecordStream(String name,
                 int orderIndex,
                 ClosableIterator<R> source,
                 long bufferBytes,
                 SizeEstimator<R> sizeEstimator) {
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

    R peek() {
      return buffer.peek();
    }

    R pop() {
      R head = buffer.pollFirst();
      if (head == null) {
        throw new NoSuchElementException("RecordStream " + name + " has no buffered head");
      }
      return head;
    }

    private void refill() {
      long sizeAccum = 0L;
      while (sizeAccum < bufferBytes && source.hasNext()) {
        R next = source.next();
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

  private final class MergeSortIterator implements ClosableIterator<R> {

    private final PriorityQueue<RecordStream<R>> heap;
    private boolean iteratorClosed = false;

    MergeSortIterator() {
      // Order primarily by the buffered head's record key; break ties by orderIndex (older first)
      // so that the merger receives streams in deterministic age order.
      Comparator<RecordStream<R>> cmp = Comparator
          .comparing((RecordStream<R> s) -> keyExtractor.apply(s.peek()))
          .thenComparingInt(s -> s.orderIndex);
      this.heap = new PriorityQueue<>(Math.max(1, streams.size()), cmp);
      for (RecordStream<R> stream : streams) {
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
    public R next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      RecordStream<R> top = heap.poll();
      String key = keyExtractor.apply(top.peek());

      // Collect every stream whose current head shares this key. We pop them off the heap so we
      // can advance and re-insert them after merging in deterministic age order.
      List<RecordStream<R>> sameKey = new ArrayList<>();
      sameKey.add(top);
      while (!heap.isEmpty() && key.equals(keyExtractor.apply(heap.peek().peek()))) {
        sameKey.add(heap.poll());
      }
      sameKey.sort(Comparator.comparingInt(s -> s.orderIndex));

      // Drain one record from each stream in older→newer order, folding via the merger.
      // Within each stream, additional adjacent same-key records (a stream is allowed to emit
      // duplicates per the issue) are also folded in — the existing key-based path always merges
      // every repeated key, so the merge-sort path must do the same.
      R merged = null;
      try {
        for (RecordStream<R> s : sameKey) {
          merged = drainAndFoldSameKey(s, key, merged);
        }
      } catch (IOException e) {
        throw new HoodieIOException("Failed to merge records sharing key " + key, e);
      }

      // Re-insert any stream that still has more buffered/unread records.
      for (RecordStream<R> s : sameKey) {
        if (s.prime()) {
          heap.offer(s);
        }
      }

      return merged;
    }

    /**
     * Pops every consecutive same-key record from {@code stream}, folding them into the running
     * accumulator via the merge function. {@code priorAcc} carries the accumulator from older
     * streams (or {@code null} when this is the first stream).
     */
    private R drainAndFoldSameKey(RecordStream<R> stream, String key, R priorAcc) throws IOException {
      R acc = priorAcc;
      do {
        R next = stream.pop();
        acc = (acc == null) ? next : mergeFn.merge(acc, next);
      } while (stream.prime() && key.equals(keyExtractor.apply(stream.peek())));
      return acc;
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

  public static <R> Builder<R> newBuilder() {
    return new Builder<>();
  }

  /** Convenience factory for streams of {@link HoodieRecord}, used by tests and Avro callers. */
  public static <T> Builder<HoodieRecord<T>> newBuilderForHoodieRecords() {
    return new Builder<HoodieRecord<T>>().withKeyExtractor(HoodieRecord::getRecordKey);
  }

  /** Convenience factory for streams of {@link BufferedRecord}, used by the file-group reader path. */
  public static <T> Builder<BufferedRecord<T>> newBuilderForBufferedRecords() {
    return new Builder<BufferedRecord<T>>().withKeyExtractor(BufferedRecord::getRecordKey);
  }

  /**
   * Builder for {@link HoodieMergeSortLogRecordScanner}. Streams must be supplied via
   * {@link #addStream} in oldest-to-newest order (the base file, when present, comes first).
   * The merge function can be set explicitly with {@link #withMergeFn}, or assembled from a
   * {@link HoodieRecordMerger} via {@link #withRecordMerger}.
   * <p>
   * Buffer configuration ({@link #withBufferBytes}, {@link #withSizeEstimator}) is captured at
   * {@link #build()} time and applied uniformly to every stream, regardless of the order in which
   * configuration and stream-registration calls were made.
   */
  public static class Builder<R> {
    private final List<NamedSource<R>> sources = new ArrayList<>();
    private long bufferBytes = DEFAULT_PER_STREAM_BUFFER_BYTES;
    private SizeEstimator<R> sizeEstimator = null;
    private RecordMergeFn<R> mergeFn;
    private Function<R, String> keyExtractor;

    public Builder<R> addStream(String name, ClosableIterator<R> source) {
      sources.add(new NamedSource<>(name, source));
      return this;
    }

    public Builder<R> withBufferBytes(long bufferBytes) {
      checkArgument(bufferBytes > 0, "bufferBytes must be positive");
      this.bufferBytes = bufferBytes;
      return this;
    }

    public Builder<R> withSizeEstimator(SizeEstimator<R> sizeEstimator) {
      this.sizeEstimator = sizeEstimator;
      return this;
    }

    public Builder<R> withMergeFn(RecordMergeFn<R> mergeFn) {
      this.mergeFn = mergeFn;
      return this;
    }

    public Builder<R> withKeyExtractor(Function<R, String> keyExtractor) {
      this.keyExtractor = keyExtractor;
      return this;
    }

    /**
     * Wires the supplied {@link HoodieRecordMerger} as the merge function for streams of
     * {@link HoodieRecord}, marshaling records through {@link BufferedRecord} the same way
     * {@link HoodieMergedLogRecordScanner} does so the streaming path produces identical merge
     * semantics to the legacy spill-map path.
     * <p>
     * Only valid when {@code R} is {@link HoodieRecord}.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> Builder<R> withRecordMerger(HoodieRecordMerger merger,
                                           RecordContext<T> recordContext,
                                           HoodieSchema readerSchema,
                                           TypedProperties payloadProps,
                                           String[] orderingFields,
                                           DeleteContext deleteContext) {
      this.mergeFn = (RecordMergeFn) (RecordMergeFn<HoodieRecord<T>>) (older, newer) -> {
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
      if (this.keyExtractor == null) {
        this.keyExtractor = (Function) (Function<HoodieRecord<T>, String>) HoodieRecord::getRecordKey;
      }
      return this;
    }

    public HoodieMergeSortLogRecordScanner<R> build() {
      checkArgument(mergeFn != null, "Either a merge function or a record merger must be supplied");
      checkArgument(keyExtractor != null,
          "A key extractor must be supplied (e.g. via withKeyExtractor or one of the static factory methods)");
      // Resolve a default size estimator only at build() time so withSizeEstimator() and
      // withBufferBytes() apply uniformly to all streams regardless of call order.
      SizeEstimator<R> resolvedEstimator = (sizeEstimator != null)
          ? sizeEstimator
          : record -> Math.max(1L, ObjectSizeCalculator.getObjectSize(record));
      List<RecordStream<R>> built = new ArrayList<>(sources.size());
      for (int i = 0; i < sources.size(); i++) {
        NamedSource<R> ns = sources.get(i);
        built.add(new RecordStream<>(ns.name, i, ns.source, bufferBytes, resolvedEstimator));
      }
      LOG.info("Building HoodieMergeSortLogRecordScanner with {} input stream(s) and per-stream buffer of {} bytes",
          built.size(), bufferBytes);
      return new HoodieMergeSortLogRecordScanner<>(built, keyExtractor, mergeFn);
    }

    private static final class NamedSource<R> {
      final String name;
      final ClosableIterator<R> source;

      NamedSource(String name, ClosableIterator<R> source) {
        this.name = name;
        this.source = source;
      }
    }
  }
}
