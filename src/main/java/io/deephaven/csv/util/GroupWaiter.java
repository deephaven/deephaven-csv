package io.deephaven.csv.util;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * This simple class submits tasks to an {@link ExecutorService} and allows the caller to wait until (1) all of them
 * complete, (2) any of them throws, whichever happens first.
 */
public class GroupWaiter {
    /**
     * The {@link ExecutorService} that runs the {@link Callable}s.
     */
    private final ExecutorService executor;
    /**
     * The number of {@link Callable}s that have been submitted via {@link GroupWaiter#submit}.
     */
    private int numSubmissions = 0;
    /**
     * The number of {@link Callable}s that have succeeded.
     */
    private int numSuccesses = 0;
    /**
     * Not null if any {@link Callable}s has thrown. Contains the first thrown {@link Throwable}.
     */
    private Throwable error;

    /**
     * Constructor.
     * 
     * @param executor the {@link ExecutorService} that runs the {@link Callable}s.
     */
    public GroupWaiter(final ExecutorService executor) {
        this.executor = executor;
    }

    /**
     * Submits a {@link Callable} to the {@link ExecutorService}, wrapping it in code which keeps track of whether it
     * ultimately succeeds or throws.
     */
    public <T> Future<T> submit(Callable<T> callable) {
        synchronized (this) {
            ++numSubmissions;
        }
        final Callable<T> wrapped = () -> {
            try {
                final T result = callable.call();
                noteSuccess();
                return result;
            } catch (Throwable t) {
                // Want to catch all Errors here and specifically OutOfMemoryError
                noteFailure(t);
                throw new CsvReaderException("Caught exception", t);
            }
        };
        return executor.submit(wrapped);
    }

    /**
     * Waits until (1) all submitted {@link Callable}s complete, or (2) any of them throws. In the latter case, this
     * method will rethrow the {@link Throwable}.
     */
    public synchronized void waitAll() throws Exception {
        while (true) {
            if (error != null) {
                throw new Exception(error);
            }
            if (numSuccesses == numSubmissions) {
                return;
            }
            wait();
        }
    }

    private synchronized void noteSuccess() {
        ++numSuccesses;
        if (numSuccesses == numSubmissions) {
            notifyAll();
        }
    }

    private synchronized void noteFailure(Throwable throwable) {
        if (error != null) {
            return;
        }
        error = throwable;
        notifyAll();
    }
}
