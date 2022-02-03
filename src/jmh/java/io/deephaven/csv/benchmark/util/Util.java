package io.deephaven.csv.benchmark.util;

import java.util.Random;
import java.util.function.IntFunction;

public class Util {
    public static <TARRAY> TARRAY[] makeArray(final int numRows, final int numCols,
            final IntFunction<TARRAY> innerArrayFactory,
            final IntFunction<TARRAY[]> outerArrayFactory) {
        final TARRAY[] result = outerArrayFactory.apply(numCols);
        for (int ii = 0; ii < numCols; ++ii) {
            result[ii] = innerArrayFactory.apply(numRows);
        }
        return result;
    }

    /**
     * Due to the weaknesses of Random this is unlikely to be evenly distributed, but we don't care.
     */
    public static long make22ndCenturyTimestamp(final Random rng) {
        final long timeRangeStart = 946684800; // 2000-01-01 00:00:00 GMT
        final long timeRangeEnd = 4102444800L; // 2100-01-01 00:00:00 GMT
        final long timeRange = timeRangeEnd - timeRangeStart;

        final long seconds = timeRangeStart + Math.abs(rng.nextLong()) % timeRange;
        final long nanos = rng.nextInt(1_000_000_000);

        return seconds * 1_000_000_000 + nanos;
    }
}
