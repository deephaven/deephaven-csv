package io.deephaven.csv.densestorage;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.util.MutableInt;

/** Companion to the {@link QueueWriter}. See the documentation there for details. */
public class QueueReader<TARRAY> {
    /** Current node. */
    private QueueNode<TARRAY> node;
    /** Current block we are reading from, extracted from the current node. */
    protected TARRAY genericBlock;
    /**
     * Current offset in the current block. Updated as we read data. When the value reaches "end", then data in this
     * block is exhausted.
     */
    protected int current;
    /** "end" offset of the current block. */
    protected int end;

    /** Constructor. */
    protected QueueReader(QueueNode<TARRAY> node) {
        this.node = node;
        this.genericBlock = null;
        this.current = 0;
        this.end = 0;
    }

    protected QueueReader(final QueueReader<TARRAY> other) {
        this.node = other.node;
        this.genericBlock = other.genericBlock;
        this.current = other.current;
        this.end = other.end;
    }

    /**
     * This method exists as a helper method for a subclass' tryGetXXX method. A typical implementation is in
     * CharReader:
     *
     * <pre>
     * {@code
     * if (current + size > end) {
     *     if (!tryRefill(size)) {
     *         return false;
     *     }
     *     typedBlock = genericBlock;
     * }
     * }
     * </pre>
     *
     * The "if" in the caller is actually checking for multiple cases in a single comparison. One is a normal "buffer
     * empty, needs to be refilled" case. The other is a bad "something went terribly wrong" case.
     *
     * <ul>
     * <li>Case 1, The "buffer empty" case. Then {@code current == end}, and therefore {@code current + size > end}
     * (assuming {@code size > 0}, which it always is). Therefore, the 'if' inside the tryGetXXX code would evaluate to
     * true, so the tryGetXXX code would call this method. Then this method refills the buffer.
     * <li>Case 2: The buffer is not empty, but A logic error (which can't happen if the code is correct) has caused the
     * requested slice to go past the end of the block. Then {@code current < end} but {@code current + size > end}.
     * Again, the 'if' inside the tryGetXXX code would evaluate to true, so the tryGetXXX code would call this method.
     * But then the first line of our method detects the past-the-end condition and throws an exception.
     * <li>Case 3: The "buffer can satisfy the request" case. Then {@code current + size <= end}, so the 'if' inside the
     * tryGetXXX code would evaluate to false, and the tryGetXXX method doesn't call this method.
     * </ul>
     */
    protected boolean tryRefill(int size) {
        if (current != end) {
            throw new RuntimeException("Logic error: slice straddled block");
        }
        while (current == end) {
            if (node.isLast) {
                // Hygeine.
                node = null;
                genericBlock = null;
                current = 0;
                end = 0;
                return false;
            }
            node = node.waitForNext();
            genericBlock = node.data;
            current = node.begin;
            end = node.end;
        }
        if (end - current < size) {
            throw new RuntimeException(
                    String.format(
                            "Logic error: got short block: expected at least %d, got %d", size, end - current));
        }
        return true;
    }

    /** A QueueReader specialized for bytes. */
    public static final class ByteReader extends QueueReader<byte[]> {
        /**
         * Typed version of the current block. Saves us some implicit casting from the generic TARRAY object. This is a
         * performance optimization that may not matter.
         */
        private byte[] typedBlock;

        /** Constructor. */
        public ByteReader(final QueueNode<byte[]> head) {
            super(head);
        }

        private ByteReader(final ByteReader other) {
            super(other);
            typedBlock = other.typedBlock;
        }

        public ByteReader copy() {
            return new ByteReader(this);
        }

        /**
         * Tries to get the next ByteSlice from the reader.
         *
         * @param size The exact number of chars to place in the slice.
         * @param bs The result, modified in place.
         * @return true If the next ByteSlice was successfully read; false if the end of input was reached.
         */
        public boolean tryGetBytes(final int size, final ByteSlice bs) {
            if (current + size > end) {
                if (!tryRefill(size)) {
                    return false;
                }
                typedBlock = genericBlock;
            }
            bs.reset(typedBlock, current, current + size);
            current += size;
            return true;
        }
    }

    /** A QueueReader specialized for ints. */
    public static final class IntReader extends QueueReader<int[]> {
        /**
         * Typed version of the current block. Saves us some implicit casting from the generic TARRAY object. This is a
         * performance optimization that may not matter.
         */
        private int[] typedBlock;

        /** Constructor. */
        public IntReader(QueueNode<int[]> head) {
            super(head);
        }

        private IntReader(final IntReader other) {
            super(other);
            typedBlock = other.typedBlock;
        }

        public IntReader copy() {
            return new IntReader(this);
        }

        /**
         * Tries to get the next integer from the reader.
         *
         * @param result If the operation succeeds, contains the next integer. Otherwise, the contents are unspecified.
         * @return true if the next value was successfully read; false if the end of input was reached.
         */
        public boolean tryGetInt(final MutableInt result) {
            if (current == end) {
                if (!tryRefill(1)) {
                    return false;
                }
                typedBlock = genericBlock;
            }
            result.setValue(typedBlock[current++]);
            return true;
        }
    }

    /** A QueueReader specialized for byte arrays. */
    public static final class ByteArrayReader extends QueueReader<byte[][]> {
        /**
         * Typed version of the current block. Saves us some implicit casting from the generic TARRAY object. This is a
         * performance optimization that may not matter.
         */
        private byte[][] typedBlock;

        public ByteArrayReader(final QueueNode<byte[][]> head) {
            super(head);
        }

        private ByteArrayReader(final ByteArrayReader other) {
            super(other);
            typedBlock = other.typedBlock;
        }

        public ByteArrayReader copy() {
            return new ByteArrayReader(this);
        }

        /**
         * Tries to get the next ByteSlice from the reader.
         *
         * @param bs The result, modified in place.
         * @return true If the next ByteSlice was successfully read; false if the end of input was reached.
         */
        public boolean tryGetBytes(final ByteSlice bs) {
            if (current == end) {
                if (!tryRefill(1)) {
                    return false;
                }
                typedBlock = genericBlock;
            }
            final byte[] data = typedBlock[current++];
            bs.reset(data, 0, data.length);
            return true;
        }
    }
}
