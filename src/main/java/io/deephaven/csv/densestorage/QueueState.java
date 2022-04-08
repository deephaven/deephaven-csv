package io.deephaven.csv.densestorage;

import io.deephaven.csv.util.Util;

/**
 * This class is used to maintain a statistic of the number of unread blocks (blocks not observed by any reader). We use
 * this so as to prevent the writer from getting too far ahead of the slowest reader.
 */
public class QueueState {
    /**
     * The unobserved block limit. When {@link QueueState#numUnobservedBlocks} reaches this limit, the caller to
     * {@link QueueState#noteBlockAdded()} will block. Callers will typically set this to
     * {@link DenseStorageConstants#MAX_UNOBSERVED_BLOCKS} in the concurrent case, and {@link Integer#MAX_VALUE} in the
     * synchronous case.
     */
    private final int maxUnobservedBlocks;
    /**
     * The number of blocks that have been appended to the queue but never read by any QueueReader.
     */
    private int numUnobservedBlocks;

    public QueueState(int maxUnobservedBlocks) {
        this.maxUnobservedBlocks = maxUnobservedBlocks;
        this.numUnobservedBlocks = 0;
    }

    public synchronized void noteBlockAdded() {
        ++numUnobservedBlocks;
        while (numUnobservedBlocks >= maxUnobservedBlocks) {
            Util.catchyWait(this);
        }
    }

    public synchronized void noteBlockObserved() {
        --numUnobservedBlocks;
        notifyAll();
    }
}
