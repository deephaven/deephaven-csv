package io.deephaven.csv.util;

/**
 * A simple wrapper class, inspired by the similar class in Apache Commons.
 */
public final class MutableObject<T> {
    private T value;

    /**
     * Constructor. Sets the contained value to null.
     */
    public MutableObject() {}

    /**
     * Constructor. Sets the contained value to the specified value.
     * 
     * @param value The specified value.
     */
    public MutableObject(final T value) {
        this.value = value;
    }

    /**
     * Sets the contained value.
     * 
     * @param newValue The new value.
     */
    public void setValue(T newValue) {
        value = newValue;
    }

    /**
     * Reads the contained value.
     * 
     * @return The contained value.
     */
    public T getValue() {
        return value;
    }
}
