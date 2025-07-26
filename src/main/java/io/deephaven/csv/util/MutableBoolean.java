package io.deephaven.csv.util;

/**
 * A simple wrapper class, inspired by the similar class in Apache Commons.
 */
public final class MutableBoolean {
    private boolean value;

    /**
     * Sets the contained value.
     * 
     * @param newValue The new value.
     */
    public void setValue(boolean newValue) {
        value = newValue;
    }

    /**
     * Reads the contained value.
     * 
     * @return The contained value.
     */
    public boolean booleanValue() {
        return value;
    }
}
