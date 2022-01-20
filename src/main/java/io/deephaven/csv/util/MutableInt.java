package io.deephaven.csv.util;

/**
 * A simple wrapper class, inspired by the similar class in Apache Commons.
 */
public final class MutableInt {
    private int value;

    public void setValue(int newValue) {
        value = newValue;
    }

    public int intValue() {
        return value;
    }
}
