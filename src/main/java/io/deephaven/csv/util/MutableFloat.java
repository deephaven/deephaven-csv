package io.deephaven.csv.util;

/**
 * A simple wrapper class, inspired by the similar class in Apache Commons.
 */
public final class MutableFloat {
    private float value;

    public void setValue(float newValue) {
        value = newValue;
    }

    public float floatValue() {
        return value;
    }
}
