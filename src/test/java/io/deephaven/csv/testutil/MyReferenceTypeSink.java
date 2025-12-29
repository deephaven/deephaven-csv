package io.deephaven.csv.testutil;

import io.deephaven.csv.sinks.Sink;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class MyReferenceTypeSink<T> implements Sink<T[]> {
    private final List<T> dest = new ArrayList<>();

    @Override
    public void write(
            T[] src, boolean[] isNull, long destBegin, long destEnd, boolean appending) {
        if (destBegin == destEnd) {
            return;
        }

        final int size = Math.toIntExact(destEnd - destBegin);
        if (appending) {
            // If the new area starts beyond the end of the destination, pad the destination.
            while (dest.size() < destBegin) {
                dest.add(null);
            }
            for (int ii = 0; ii < size; ++ii) {
                dest.add(isNull[ii] ? null : src[ii]);
            }
            return;
        }

        final int destBeginAsInt = Math.toIntExact(destBegin);
        for (int ii = 0; ii < size; ++ii) {
            dest.set(destBeginAsInt + ii, isNull[ii] ? null : src[ii]);
        }
    }

    @Override
    public Object getUnderlying() {
        return dest;
    }
}
