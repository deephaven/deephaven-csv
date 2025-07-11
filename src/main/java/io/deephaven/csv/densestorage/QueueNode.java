package io.deephaven.csv.densestorage;

import java.util.concurrent.Semaphore;

/**
 * Linked list node that holds data for a {@link DenseStorageWriter} or {@link DenseStorageReader}. All fields are
 * immutable except the "next" field. Synchronization for reading/writing the "next" field is managed by the
 * {@link DenseStorageWriter} and {@link DenseStorageReader}.
 */
public final class QueueNode<TARRAY> {
    public static <TARRAY> QueueNode<TARRAY> createInitial(int maxUnobservedBlocks) {
        final Semaphore semaphore = new Semaphore(maxUnobservedBlocks);
        return new QueueNode<>(semaphore, null, 0, 0, false);
    }

    /**
     * The shared semaphore which prevents the writer from getting too far ahead of the reader.
     */
    public final Semaphore semaphore;
    public final TARRAY data;
    public final int begin;
    public final int end;
    public final boolean isLast;
    /** Readers and writers of this field have arranged to synchronize with each other. */
    public QueueNode<TARRAY> next;
    /**
     * Whether a reader has already observed the {@link QueueNode#next} field transitioning from null to non-null.
     */
    private boolean observed;

    /**
     * Constructor. Sets this queue node to represent the half-open interval ['begin','end') of the array 'data'.
     */
    private QueueNode(final Semaphore semaphore, TARRAY data, int begin, int end, boolean isLast) {
        this.semaphore = semaphore;
        this.data = data;
        this.begin = begin;
        this.end = end;
        this.isLast = isLast;
        this.next = null;
    }

    public QueueNode<TARRAY> appendNextMaybeWait(TARRAY data, int begin, int end, boolean isLast) {
        try {
            semaphore.acquire(1);
        } catch (InterruptedException ie) {
            throw new RuntimeException("Thread interrupted", ie);
        }
        synchronized (this) {
            if (next != null) {
                throw new RuntimeException("next is already set");
            }
            // New node sharing the same semaphore.
            next = new QueueNode<>(semaphore, data, begin, end, isLast);
            notifyAll();
            return next;
        }
    }

    /**
     * Get a non-null 'next' field. Will block until the next field is non-null. If this is the first time
     *
     * @return True if this call was the first to observe the {@link QueueNode#next} field going from null to non-null.
     *         Otherwise returns false.
     */
    public QueueNode<TARRAY> waitForNext() {
        boolean needsRelease;
        synchronized (this) {
            while (next == null) {
                try {
                    wait();
                } catch (InterruptedException ie) {
                    throw new RuntimeException("Thread interrupted", ie);
                }
            }
            needsRelease = !observed;
            observed = true;
        }
        if (needsRelease) {
            semaphore.release();
        }
        return next;
    }
}
