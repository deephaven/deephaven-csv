package io.deephaven.csv.util;

/**
 * This class implements a kind of "moveable" semantics. It exists because we really want the garbage collector to be
 * able to clean up (prefixes of) our DenseStorage linked list when it can; and we don't want some careless programming
 * error to leave a reference lying around that prevents it from doing so. This class makes it somewhat convenient to
 * have single-owner and "move" semantics, so it's a little easier to not leak references.
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
