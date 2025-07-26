package io.deephaven.csv.util;

/**
 * A simple wrapper class, inspired by the similar class in Apache Commons.
 */
public final class MutableInt {
    private int value;

    /**
     * Sets the contained value.
     * 
     * @param newValue The new value.
     */
    public void setValue(int newValue) {
        value = newValue;
    }

    /**
     * Reads the contained value.
     * 
     * @return The contained value.
     */
    public int intValue() {
        return value;
    }
}
