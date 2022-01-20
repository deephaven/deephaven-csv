package io.deephaven.csv.util;

/**
 * A simple wrapper class, inspired by the similar class in Apache Commons.
 */
public final class MutableBoolean {
    private boolean value;

    public void setValue(boolean newValue) {
        value = newValue;
    }

    public boolean booleanValue() {
        return value;
    }
}
