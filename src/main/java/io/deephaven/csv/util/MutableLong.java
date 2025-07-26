package io.deephaven.csv.util;


/**
 * A simple wrapper class, inspired by the similar class in Apache Commons.
 */
public final class MutableLong {
    private long value;

    /**
     * Sets the contained value.
     * 
     * @param newValue The new value.
     */
    public void setValue(long newValue) {
        value = newValue;
    }

    /**
     * Reads the contained value, cast to an integer.
     * 
     * @return The contained value.
     */
    public int intValue() {
        return (int) value;
    }

    /**
     * Reads the contained value.
     * 
     * @return The contained value.
     */
    public long longValue() {
        return value;
    }
}
