package io.deephaven.csv.testutil;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.Source;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class StubbornSink<TARRAY> implements Sink<TARRAY>, Source<TARRAY> {
    private final long timeout;
    private final CountDownLatch outgoingSinkReady;
    private final CountDownLatch incomingShutdownRequest;
    private final CountDownLatch outgoingShutdownComplete;

    public StubbornSink(Duration timeout, CountDownLatch outgoingSinkReady,
            CountDownLatch incomingShutdownRequest, CountDownLatch outgoingShutdownComplete) {
        this.timeout = timeout.toMillis();
        this.outgoingSinkReady = outgoingSinkReady;
        this.incomingShutdownRequest = incomingShutdownRequest;
        this.outgoingShutdownComplete = outgoingShutdownComplete;
    }

    @Override
    public void write(TARRAY src, boolean[] isNull, long destBegin, long destEnd, boolean appending) {
        if (destBegin == destEnd) {
            return;
        }

        // Notify other cooperating sink that we are inside our "write" stage.
        outgoingSinkReady.countDown();

        final long expirationTime = System.currentTimeMillis() + timeout;
        while (true) {
            long remainingTime = expirationTime - System.currentTimeMillis();
            try {
                boolean ignored = incomingShutdownRequest.await(remainingTime, TimeUnit.MILLISECONDS);
                // Exit infinite loop, regardless of message received or timeout,
                break;
            } catch (InterruptedException e) {
                // Ignore attempts to interrupt me earlier than the timeout.
            }
        }
        outgoingShutdownComplete.countDown();
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
