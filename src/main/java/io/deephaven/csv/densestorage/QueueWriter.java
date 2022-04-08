package io.deephaven.csv.densestorage;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.util.Pair;

import java.util.function.IntFunction;

/**
 * The various QueueWriters ({@link ByteWriter}, {@link IntWriter}, etc.) work in tandem with their corresponding
 * {@link QueueReader}s ({@link QueueReader.ByteReader}, {@link QueueReader.IntReader}, etc), forming a FIFO queue. The
 * QueueWriter writes data, and the {@link QueueReader} reads that data. If the {@link QueueReader} "catches up", it
 * will block until the QueueWriter provides more data, or indicates that it is done (via the {@link #finish()} method.
 * This synchronization is done at "block" granularity, so the {@link QueueReader} can only proceed when the QueueWriter
 * has written at least a "block" of data or is done. We allow multiple independent {@link QueueReader}s to consume the
 * same underlying data. In our implementation this is used so our type inferencer can take a second "pass" over the
 * same input data.
 *
 * <p>
 * In our implementation the {@link DenseStorageWriter} and {@link DenseStorageReader} are built out of various
 * QueueWriters and {@link QueueReader}s. This explains why the semantics of {@link DenseStorageWriter} and
 * {@link DenseStorageReader} are similar to those of the underlying QueueWriters and {@link QueueReader}s.
 */
public class QueueWriter<TARRAY> {
    /**
     * Queue state object which synchronizes access to the "next" fields of every node in our linked list and also keeps
     * track of how far the writer is ahead of the reader. Shared with the QueueReaders.
     */
    protected final QueueState queueState;
    /** Tail of the linked list. We append here when we flush. */
    protected QueueNode<TARRAY> tail;
    /** Size of the chunks we allocate that we pack data into. */
    protected final int blockSize;
    /** Lambda for allocating arrays for our chunks. */
    private final IntFunction<TARRAY> arrayFactory;
    /** Current block we writing to. When we flush, we will write it to a new linked list node. */
    private TARRAY genericBlock;
    /**
     * Start of the current block. This is typically 0, but not always. If the caller does an early flush (before the
     * block is filled), you can end up with multiple linked list nodes sharing different segments (slices) of the same
     * underlying array.
     */
    protected int begin;
    /**
     * Current offset in the current block. Updated as we write data. When the value reaches "end", then data in this
     * block is exhausted.
     */
    protected int current;
    /** End of the current block. The same as genericBlock.length. */
    protected int end;

    /** Constructor. */
    protected QueueWriter(final int blockSize, final IntFunction<TARRAY> arrayFactory, final boolean concurrent) {
        final int maxUnobservedBlocks = concurrent ? DenseStorageConstants.MAX_UNOBSERVED_BLOCKS : Integer.MAX_VALUE;
        this.queueState = new QueueState(maxUnobservedBlocks);
        // Creating the linked list with a sentinel object makes linked list manipulation code simpler.
        this.tail = new QueueNode<>(null, 0, 0, false);
        this.blockSize = blockSize;
        this.arrayFactory = arrayFactory;
        this.genericBlock = null;
        this.begin = 0;
        this.current = 0;
        this.end = 0;
    }

    /** Caller is finished writing. */
    public void finish() {
        flush(true);
        genericBlock = null; // hygeine
        begin = 0;
        current = 0;
        end = 0;
    }

    /**
     * This supports an "early flush" for callers like {@link DenseStorageWriter} who want to flush all their queues
     * from time to time.
     */
    public void flush() {
        flush(false);
    }

    /**
     * Flush can be called at any time... when the block is empty (and hence nothing to flush), when there's some data,
     * or when the data is full.
     *
     * @param isLast Whether this is the last node in the linked list.
     */
    private void flush(boolean isLast) {
        // Sometimes our users ask us to flush even if there is nothing to flush.
        // If the block is an "isLast" block, we need to flush it regardless of whether it contains
        // data. Otherwise (if the block is not an "isLast" block), we only flush it if it
        // contains data.
        if (!isLast && (current == begin)) {
            // No need to flush.
            return;
        }

        final QueueNode<TARRAY> newNode = new QueueNode<>(genericBlock, begin, current, isLast);
        // If this is an early flush (before the block was filled), the next node may share
        // the same underlying storage array (but disjoint segments of that array) as the current node.
        // To accomplish this, we just advance "begin" to "current" here. At this point in the logic
        // we don't care if that leaves the block with zero capacity (begin == end) or not. The decision
        // to actually start a new block is done by the addXXX code in our subclasses which eventually
        // calls flushAndAllocate.
        begin = current;
        tail.setNext(newNode);
        tail = newNode;
        queueState.noteBlockAdded();
    }

    /**
     * This method exists as a helper method for a subclass' addXXX method. A typical implementation is in CharWriter:
     *
     * <pre>
     * {@code
     * final int sliceSize = cs.size();
     * final boolean flushHappened = current + sliceSize > end;
     * if (flushHappened) {
     *   typedBlock = flushAndAllocate(sliceSize);
     * }
     * ...
     * }
     * </pre>
     *
     * The "flushHappened" variable (which at the point of its definition would be more precisely interpreted as "flush
     * is about to happen") calculates whether the data that currently needs to be written can fit in the current block
     * or not. If it can fit, the code continues on to write its data. If it can't fit, the subclass calls this
     * flushAndAllocate method to flush the current block to the linked list and allocate a new one. The new block so
     * allocated is guaranteed to have at be of size at least 'sizeNeeded'.
     */
    protected final TARRAY flushAndAllocate(int sizeNeeded) {
        flush(false);
        final int capacity = Math.max(blockSize, sizeNeeded);
        genericBlock = arrayFactory.apply(capacity);
        begin = 0;
        current = 0;
        end = capacity;
        return genericBlock;
    }

    /** A QueueWriter specialized for bytes. */
    public static final class ByteWriter extends QueueWriter<byte[]> {
        public static Pair<ByteWriter, QueueReader.ByteReader> create(final int blockSize, final boolean concurrent) {
            final ByteWriter writer = new ByteWriter(blockSize, concurrent);
            final QueueReader.ByteReader reader = new QueueReader.ByteReader(writer.queueState, writer.tail);
            return new Pair<>(writer, reader);
        }

        private byte[] typedBlock = null;

        private ByteWriter(final int blockSize, final boolean concurrent) {
            super(blockSize, byte[]::new, concurrent);
        }

        /**
         * Add bytes from a ByteSlice to the queue.
         *
         * @return true if the add caused a flush to happen prior to the write, false if no flush happened.
         */
        public boolean addBytes(ByteSlice bs) {
            final int sliceSize = bs.size();
            if (sliceSize == 0) {
                return false;
            }
            final boolean flushHappened = current + sliceSize > end;
            if (flushHappened) {
                typedBlock = flushAndAllocate(sliceSize);
            }
            bs.copyTo(typedBlock, current);
            current += sliceSize;
            return flushHappened;
        }
    }

    /** A QueueWriter specialized for ints. */
    public static final class IntWriter extends QueueWriter<int[]> {
        public static Pair<IntWriter, QueueReader.IntReader> create(final int blockSize, final boolean concurrent) {
            final IntWriter writer = new IntWriter(blockSize, concurrent);
            final QueueReader.IntReader reader = new QueueReader.IntReader(writer.queueState, writer.tail);
            return new Pair<>(writer, reader);
        }

        private int[] typedBlock = null;

        private IntWriter(final int blockSize, final boolean concurrent) {
            super(blockSize, int[]::new, concurrent);
        }

        /**
         * Add an int to the queue.
         *
         * @return true if the add caused a flush to happen prior to the write, false if no flush happened.
         */
        public boolean addInt(int value) {
            final boolean flushHappened = current == end;
            if (flushHappened) {
                typedBlock = flushAndAllocate(1);
            }
            typedBlock[current++] = value;
            return flushHappened;
        }
    }

    /** A QueueWriter specialized for byte arrays. */
    public static final class ByteArrayWriter extends QueueWriter<byte[][]> {
        public static Pair<ByteArrayWriter, QueueReader.ByteArrayReader> create(final int blockSize,
                final boolean concurrent) {
            final ByteArrayWriter writer = new ByteArrayWriter(blockSize, concurrent);
            final QueueReader.ByteArrayReader reader = new QueueReader.ByteArrayReader(writer.queueState, writer.tail);
            return new Pair<>(writer, reader);
        }

        private byte[][] block = null;

        private ByteArrayWriter(int blockSize, final boolean concurrent) {
            super(blockSize, byte[][]::new, concurrent);
        }

        /**
         * Add a byte array to the queue.
         *
         * @return true if the add caused a flush to happen prior to the write, false if no flush happened.
         */
        public boolean addByteArray(byte[] value) {
            final boolean flushHappened = current == end;
            if (flushHappened) {
                block = flushAndAllocate(1);
            }
            block[current++] = value;
            return flushHappened;
        }
    }
}
