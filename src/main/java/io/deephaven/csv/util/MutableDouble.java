package io.deephaven.csv.util;

/**
 * A simple wrapper class, inspired by the similar class in Apache Commons.
 */
public final class MutableDouble {
    private double value;

    /**
     * Sets the contained value.
     * 
     * @param newValue The new value.
     */
    public void setValue(double newValue) {
        value = newValue;
    }

    /**
     * Reads the contained value.
     * 
     * @return The contained value.
     */
    public double doubleValue() {
        return value;
    }
}
