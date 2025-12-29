package io.deephaven.csv.testutil;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.Source;

import java.util.concurrent.CountDownLatch;

public class ThrowingSink<TARRAY> implements Sink<TARRAY>, Source<TARRAY> {
    private final CountDownLatch incomingSinkReady;

    public ThrowingSink(CountDownLatch incomingSinkReady) {
        this.incomingSinkReady = incomingSinkReady;
    }

    @Override
    public void write(TARRAY src, boolean[] isNull, long destBegin, long destEnd, boolean appending) {
        if (destBegin == destEnd) {
            return;
        }
        try {
            incomingSinkReady.await();
            throw new RuntimeException("synthetic error for testing");
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted");
        }
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
