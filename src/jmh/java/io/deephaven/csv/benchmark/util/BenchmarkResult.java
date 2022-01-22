package io.deephaven.csv.benchmark.util;

public class BenchmarkResult<TARRAY> {
    public static <TARRAY> BenchmarkResult<TARRAY> of(final long numRows, final TARRAY... columns) {
        return new BenchmarkResult<>(numRows, columns);
    }

    private final long numRows;
    private final TARRAY[] columns;

    public BenchmarkResult(long numRows, TARRAY[] columns) {
        this.numRows = numRows;
        this.columns = columns;
    }

    public final long numRows() {
        return numRows;
    }

    public TARRAY[] columns() {
        return columns;
    }
}
