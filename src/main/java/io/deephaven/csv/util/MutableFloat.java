package io.deephaven.csv.util;

/**
 * A simple wrapper class, inspired by the similar class in Apache Commons.
 */
public final class MutableFloat {
    private float value;

    /**
     * Sets the contained value.
     * 
     * @param newValue The new value.
     */
    public void setValue(float newValue) {
        value = newValue;
    }

    /**
     * Reads the contained value.
     * 
     * @return The contained value.
     */
    public float floatValue() {
        return value;
    }
}
