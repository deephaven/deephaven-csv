package io.deephaven.csv.util;

/**
 * A simple wrapper class, inspired by the similar class in Apache Commons.
 */
public final class MutableDouble {
    private double value;

    public void setValue(double newValue) {
        value = newValue;
    }

    public double doubleValue() {
        return value;
    }
}
