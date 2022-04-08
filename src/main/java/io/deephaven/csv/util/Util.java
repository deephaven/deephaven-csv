package io.deephaven.csv.util;

public class Util {
    /** Call Object.wait() but suppress the need to deal with checked InterruptedExceptions. */
    public static void catchyWait(Object o) {
        try {
            o.wait();
        } catch (InterruptedException ie) {
            throw new RuntimeException(
                    "Thread interrupted: probably cancelled in the CsvReader class due to some other exception.");
        }
    }

}
