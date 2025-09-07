package io.deephaven.csv.densestorage;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.csv.util.MutableInt;

import java.util.concurrent.Semaphore;

/** Companion to the {@link DenseStorageWriter}. See the documentation there for details. */
public final class DenseStorageReader {
    private final Object syncRoot;
    private final Semaphore semaphore;
    private QueueNode tail;

    private byte[] packedBuffer = null;
    private int packedCurrent = 0;
    private int packedEnd = 0;

    private byte[][] largeArrayBuffer = null;
    private int largeArrayCurrent = 0;
    private int largeArrayEnd = 0;

    /** Constructor. */
    public DenseStorageReader(Object syncRoot, Semaphore semaphore, QueueNode head) {
        this.syncRoot = syncRoot;
        this.semaphore = semaphore;
        this.tail = head;
    }

    /**
     * Constructor that copies the state of 'other'.
     * 
     * @param other The other object
     */
    private DenseStorageReader(DenseStorageReader other) {
        this.syncRoot = other.syncRoot;
        this.semaphore = other.semaphore;
        this.tail = other.tail;
        this.packedBuffer = other.packedBuffer;
        this.packedCurrent = other.packedCurrent;
        this.packedEnd = other.packedEnd;
        this.largeArrayBuffer = other.largeArrayBuffer;
        this.largeArrayCurrent = other.largeArrayCurrent;
        this.largeArrayEnd = other.largeArrayEnd;
    }

    public DenseStorageReader copy() {
        return new DenseStorageReader(this);
    }

    /**
     * Tries to get the next slice. To do this, it fetches a 'control word' from the 'packed' subqueue. This control
     * word indicates one of three things:
     * <ol>
     * <li>End of stream</li>
     * <li>That the data for the slice is immediately following on the same subqueue, namely the 'packed' subqueue</li>
     * <li>That the data for the slice is on the other subqueue, namely the 'largeArray' subqueue.</li>
     * </ol>
     * 
     * @param bs If the method returns true, the contents of this parameter will be updated.
     * @return true if there is more data, and the ByteSlice has been populated. Otherwise, false.
     */
    public boolean tryGetNextSlice(final ByteSlice bs) throws CsvReaderException {
        final int control = getControlWord(bs);
        if (control == DenseStorageConstants.END_OF_STREAM_SENTINEL) {
            return false;
        }
        if (control == DenseStorageConstants.LARGE_BYTE_ARRAY_SENTINEL) {
            getSliceFromLargeArray(bs);
        } else {
            getSliceFromPackedArray(bs, control);
        }
        return true;
    }

    private int getControlWord(ByteSlice bs) throws CsvReaderException {
        getSliceFromPackedArray(bs, 4);
        final byte[] data = bs.data();
        final int begin = bs.begin();
        return (data[begin] & 0xff) |
                ((data[begin + 1] & 0xff) << 8) |
                ((data[begin + 2] & 0xff) << 16) |
                ((data[begin + 3] & 0xff) << 24);
    }

    private void getSliceFromLargeArray(ByteSlice bs) throws CsvReaderException {
        while (largeArrayCurrent == largeArrayEnd) {
            refill();
        }
        byte[] slice = largeArrayBuffer[largeArrayCurrent++];
        bs.reset(slice, 0, slice.length);
    }

    private void getSliceFromPackedArray(ByteSlice bs, int sizeNeeded) throws CsvReaderException {
        if (sizeNeeded == 0) {
            // If requested size is 0, then no bytes need to be pulled from the queue.
            // In fact, trying to pull 0 bytes might trigger a premature move to the next
            // node, which we don't want. Just return an empty slice.
            bs.reset(packedBuffer, packedCurrent, packedCurrent);
            return;
        }

        while (packedCurrent == packedEnd) {
            refill();
        }
        if (packedCurrent + sizeNeeded > packedEnd) {
            int availableSize = packedEnd - packedCurrent;
            throw new CsvReaderException(
                    String.format(
                            "Assertion failure: got short block: expected at least %d, got %d", sizeNeeded,
                            availableSize));
        }
        final int packedEnd = packedCurrent + sizeNeeded;
        bs.reset(packedBuffer, packedCurrent, packedEnd);
        packedCurrent = packedEnd;
    }

    private void refill() throws CsvReaderException {
        if (packedCurrent != packedEnd || largeArrayCurrent != largeArrayEnd) {
            throw new CsvReaderException("Assertion failure: refill() discarding unread data");
        }

        boolean needsRelease;
        synchronized (syncRoot) {
            while (tail.next == null) {
                try {
                    syncRoot.wait();
                } catch (InterruptedException ie) {
                    throw new RuntimeException("Thread interrupted", ie);
                }
            }

            needsRelease = !tail.appendHasBeenObserved;
            tail.appendHasBeenObserved = true;
            tail = tail.next;
        }
        if (needsRelease) {
            semaphore.release();
        }

        packedBuffer = tail.packedBuffer;
        packedCurrent = tail.packedBegin;
        packedEnd = tail.packedEnd;

        largeArrayBuffer = tail.largeArrayBuffer;
        largeArrayCurrent = tail.largeArrayBegin;
        largeArrayEnd = tail.largeArrayEnd;;
    }
}
