package io.deephaven.csv.testutil;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.Source;

public class SynchronizingSink<TARRAY> implements Sink<TARRAY>, Source<TARRAY> {
    private final int colNum;
    private final SyncState syncState;
    private long sizeWritten = 0;

    public SynchronizingSink(int colNum, SyncState syncState) {
        this.colNum = colNum;
        this.syncState = syncState;
    }

    @Override
    public void write(TARRAY src, boolean[] isNull, long destBegin, long destEnd, boolean appending) {
        sizeWritten += destEnd - destBegin;
        syncState.maybeWait(colNum, sizeWritten);
    }

    @Override
    public Object getUnderlying() {
        return null;
    }

    @Override
    public void read(TARRAY dest, boolean[] isNull, long srcBegin, long srcEnd) {
        // Do nothing.
    }
}
