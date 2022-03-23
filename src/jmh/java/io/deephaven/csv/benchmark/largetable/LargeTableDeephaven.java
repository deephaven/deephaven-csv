package io.deephaven.csv.benchmark.largetable;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.benchmark.util.SinkFactories;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.SinkFactory;

import java.io.InputStream;

public class LargeTableDeephaven {
    public static Results read(final InputStream in, final Results results) throws Exception {
        final SinkFactory sinkFactory = SinkFactories.makeRecyclingSinkFactory(
                new byte[][] {results.boolsAsBytes},
                null,
                new long[][] {results.longs0, results.longs1},
                new double[][] {results.doubles0, results.doubles1, results.doubles2},
                new String[][] {results.strings},
                new long[][] {results.timestamps});
        final CsvSpecs specs = CsvSpecs.builder()
                .hasHeaderRow(true)
                .putParserForIndex(1, Parsers.DATETIME)
                .putParserForIndex(2, Parsers.STRING)
                .putParserForIndex(3, Parsers.BOOLEAN)
                .putParserForIndex(4, Parsers.LONG)
                .putParserForIndex(5, Parsers.LONG)
                .putParserForIndex(6, Parsers.DOUBLE)
                .putParserForIndex(7, Parsers.DOUBLE)
                .putParserForIndex(8, Parsers.DOUBLE)
                .build();
        final CsvReader.Result result = CsvReader.read(specs, in, sinkFactory);
        final Object[] sinks = result.columns();
        return new Results(
                (long[]) sinks[0],
                (String[]) sinks[1],
                (byte[]) sinks[2],
                (long[]) sinks[3],
                (long[]) sinks[4],
                (double[]) sinks[5],
                (double[]) sinks[6],
                (double[]) sinks[7]);
    }
}
