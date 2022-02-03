package io.deephaven.csv.benchmark.util;

public interface ArrayBacked<TARRAY> {
    TARRAY getUnderlyingArray();
}
