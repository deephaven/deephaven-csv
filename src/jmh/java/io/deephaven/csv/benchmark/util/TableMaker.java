package io.deephaven.csv.benchmark.util;

import io.deephaven.csv.util.Renderer;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.function.IntFunction;

public final class TableMaker<TARRAY> {
    private final String[] headers;
    private final byte[] bytes;
    private final TARRAY[] expected;

    public TableMaker(final Random rng, final int rows, final int cols, final IntFunction<TARRAY> innerArrayFactory,
            final IntFunction<TARRAY[]> outerArrayFactory, final Generator<TARRAY> generator,
            final CellRenderer<TARRAY> cellRenderer) {
        headers = new String[cols];
        for (int c = 0; c < cols; ++c) {
            headers[c] = "Col" + (c + 1);
        }
        expected = outerArrayFactory.apply(cols);
        for (int c = 0; c < cols; ++c) {
            final TARRAY col = innerArrayFactory.apply(rows);
            generator.apply(rng, col, 0, rows);
            expected[c] = col;
        }

        final StringBuilder sb = new StringBuilder();
        sb.append(String.join(",", headers)).append('\n');

        for (int r = 0; r < rows; ++r) {
            for (int c = 0; c < cols; ++c) {
                if (c != 0) {
                    sb.append(',');
                }
                cellRenderer.apply(sb, expected[c], r);
            }
            sb.append('\n');
        }

        bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    public void check(final TARRAY[] actual) {
        if (!equalsActual(actual)) {
            throw new RuntimeException("expected != actual");
        }
    }

    private boolean equalsActual(final TARRAY[] actual) {
        return Arrays.deepEquals(expected, actual);
    }

    public InputStream makeStream() {
        return new ByteArrayInputStream(bytes);
    }

    public String[] headers() {
        return headers;
    }

    public interface Generator<TARRAY> {
        void apply(Random rng, TARRAY column, int begin, int end);
    }

    public interface CellRenderer<TARRAY> {
        void apply(StringBuilder sb, TARRAY col, int rowNum);
    }
}
