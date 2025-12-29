package io.deephaven.csv.testutil;

public class SyncState {
    private final int numParticipatingColumns;
    private final long thresholdSize;
    private long nextThreshold = 0;
    private int numWaiters = 0;

    public SyncState(int numParticipatingColumns, long thresholdSize) {
        this.numParticipatingColumns = numParticipatingColumns;
        this.thresholdSize = thresholdSize;
    }

    public synchronized void maybeWait(final int colNum, final long sizeWritten) {
        while (true) {
            if (sizeWritten < nextThreshold) {
                return;
            }

            if (numWaiters == numParticipatingColumns - 1) {
                // Everyone else is waiting, so advance the threshold.
                nextThreshold += thresholdSize;
                notifyAll();
                continue;
            }

            ++numWaiters;
            try {
                wait();
            } catch (InterruptedException ie) {
                throw new RuntimeException("Interrupted", ie);
            } finally {
                --numWaiters;
            }
        }
    }
}
