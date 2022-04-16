package io.deephaven.csv.util;

/**
 * A simple wrapper class, inspired by the similar class in Apache Commons.
 */
public final class MutableObject<T> {
    private T value;

    public MutableObject() {}

    public MutableObject(final T value) {
        this.value = value;
    }

    public void setValue(T newValue) {
        value = newValue;
    }

    public T getValue() {
        return value;
    }
}
