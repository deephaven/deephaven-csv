package io.deephaven.csv;

import io.deephaven.csv.parsers.*;
import io.deephaven.csv.testutil.*;
import io.deephaven.csv.util.CsvReaderException;
import org.junit.jupiter.api.Test;

import java.util.*;

public class PerColumnParsersTest {
    /**
     * Tests per-column custom parser inferencing.
     */
    @Test
    public void testPerColumnParsers() throws CsvReaderException {
        // Has four columns with the same values: 0, 10, 5000
        // The columns are configured with different parser inferencers
        // Col0: byte, string
        // Col1: byte, short, string
        // Col2: byte, int, string
        // Col3: default parser, which is set to byte, double, string
        final String input = "Col0,Col1,Col2,Col3\n" +
                "0, 0, 0, 0\n" +
                "10, 10, 10, 10\n" +
                "5000, 5000, 5000, 5000\n";

        final List<Parser<?>> col0Parsers = Arrays.asList(Parsers.BYTE, Parsers.STRING);
        final List<Parser<?>> col1Parsers = Arrays.asList(Parsers.BYTE, Parsers.SHORT, Parsers.STRING);
        final List<Parser<?>> col2Parsers = Arrays.asList(Parsers.BYTE, Parsers.INT, Parsers.STRING);
        final List<Parser<?>> defaultParsers = Arrays.asList(Parsers.BYTE, Parsers.DOUBLE, Parsers.STRING);

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Col0", "0", "10", "5000"),
                        Column.ofValues("Col1", (short) 0, (short) 10, (short) 5000),
                        Column.ofValues("Col2", (int) 0, (int) 10, (int) 5000),
                        Column.ofValues("Col3", (double) 0, (double) 10, (double) 5000));

        CsvTestUtil.invokeTests(CsvTestUtil.defaultCsvBuilder()
                .putParsersForIndex(0, col0Parsers)
                .putParsersForIndex(1, col1Parsers)
                .putParsersForIndex(2, col2Parsers)
                .parsers(defaultParsers)
                .build(), input, expected);
    }
}
