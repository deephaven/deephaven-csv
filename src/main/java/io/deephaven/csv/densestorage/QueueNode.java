package io.deephaven.csv.densestorage;

/**
 * Linked list node that holds data for the communication between a {@link DenseStorageWriter} and
 * {@link DenseStorageReader}. All fields are immutable except the "next" and "appendHasBeenObserved" fields.
 * Synchronization for reading/writing the "next" field is managed by the {@link DenseStorageWriter} and
 * {@link DenseStorageReader}.
 */
public final class QueueNode {
    /**
     * Represents a slice of packed bytes in the packed buffer. It is the contract of the code that bytes in this slice
     * are immutable.
     */
    public final byte[] packedBuffer;
    public final int packedBegin;
    public final int packedEnd;

    /**
     * Represents a slice of byte[] references in the large array buffer. It is the contract of the code that the byte[]
     * references in this interval (and the bytes they point to) are immutable.
     */
    public final byte[][] largeArrayBuffer;
    public final int largeArrayBegin;
    public final int largeArrayEnd;

    public QueueNode next = null;

    /**
     * Whether at least one reader has observed the {@link QueueNode#next} field transitioning from null to non-null.
     * This is used for flow control, so that the writer doesn't get too far ahead of the reader.
     */
    public boolean appendHasBeenObserved = false;

    /**
     * Constructor. Sets this queue node to represent the passed-in slices.
     */
    public QueueNode(byte[] packedBuffer, int packedBegin, int packedEnd, byte[][] largeArrayBuffer,
            int largeArrayBegin,
            int largeArrayEnd) {
        this.packedBuffer = packedBuffer;
        this.packedBegin = packedBegin;
        this.packedEnd = packedEnd;
        this.largeArrayBuffer = largeArrayBuffer;
        this.largeArrayBegin = largeArrayBegin;
        this.largeArrayEnd = largeArrayEnd;
    }
}
