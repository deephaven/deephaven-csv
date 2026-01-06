package io.deephaven.csv.testutil;

public interface MakeCustomColumn {
    Column<?> apply(String name, Object column, int size);
}
