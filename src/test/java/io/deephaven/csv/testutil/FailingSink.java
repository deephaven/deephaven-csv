package io.deephaven.csv.testutil;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.Source;

public class FailingSink<TARRAY> implements Sink<TARRAY>, Source<TARRAY> {
    private final int colNum;

    public FailingSink(int colNum) {
        this.colNum = colNum;
    }

    @Override
    public void write(TARRAY src, boolean[] isNull, long destBegin, long destEnd, boolean appending) {
        throw new RuntimeException("synthetic error for testing: out of memory");
    }

    /**
     * For the sake of one of our unit tests, we return the colNum as our underlying.
     */
    @Override
    public Object getUnderlying() {
        return colNum;
    }

    @Override
    public void read(TARRAY dest, boolean[] isNull, long srcBegin, long srcEnd) {
        // Do nothing.
    }
}
