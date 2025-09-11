package io.deephaven.csv.util;

/**
 * A simple class representing a pair.
 * 
 * @param <FIRST> The type of the first element of the pair.
 * @param <SECOND> The type of the second element of the pair.
 */
public class Pair<FIRST, SECOND> {
    /**
     * The first element of the pair.
     */
    public final FIRST first;
    /**
     * The second element of the pair.
     */
    public final SECOND second;

    /**
     * Constructor
     * 
     * @param first The first element of the pair.
     * @param second The second element of the pair.
     */
    public Pair(FIRST first, SECOND second) {
        this.first = first;
        this.second = second;
    }
}
