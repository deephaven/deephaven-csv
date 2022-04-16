package io.deephaven.csv.util;

/**
 * This class implements a kind of "moveable" semantics.
 * 
 * @param <T>
 */
public final class Moveable<T> {
    private T obj;

    public Moveable(T obj) {
        this.obj = obj;
    }

    public T get() {
        return obj;
    }

    public void reset() {
        obj = null;
    }

    public Moveable<T> move() {
        final Moveable<T> result = new Moveable<>(obj);
        obj = null;
        return result;
    }
}
