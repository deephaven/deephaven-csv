package io.deephaven.csv.util;


/**
 * A simple wrapper class, inspired by the similar class in Apache Commons.
 */
public final class MutableLong {
    private long value;

    public void setValue(long newValue) {
        value = newValue;
    }

    public int intValue() {
        return (int) value;
    }

    public long longValue() {
        return value;
    }
}
