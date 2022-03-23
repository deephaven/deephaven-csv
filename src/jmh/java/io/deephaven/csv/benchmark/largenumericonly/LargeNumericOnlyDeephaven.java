package io.deephaven.csv.benchmark.largenumericonly;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.benchmark.util.SinkFactories;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.SinkFactory;

import java.io.InputStream;

public class LargeNumericOnlyDeephaven {
    public static Results read(final InputStream in, final Results results) throws Exception {
        final SinkFactory sinkFactory = SinkFactories.makeRecyclingSinkFactory(
                null,
                null,
                new long[][] {results.longs0, results.longs1, results.longs2, results.longs3},
                new double[][] {results.doubles0, results.doubles1, results.doubles2, results.doubles3},
                null,
                null);

        final CsvSpecs specs = CsvSpecs.builder()
                .hasHeaderRow(true)
                .putParserForIndex(1, Parsers.LONG)
                .putParserForIndex(2, Parsers.LONG)
                .putParserForIndex(3, Parsers.LONG)
                .putParserForIndex(4, Parsers.LONG)
                .putParserForIndex(5, Parsers.DOUBLE)
                .putParserForIndex(6, Parsers.DOUBLE)
                .putParserForIndex(7, Parsers.DOUBLE)
                .putParserForIndex(8, Parsers.DOUBLE)
                .build();
        final CsvReader.Result result = CsvReader.read(specs, in, sinkFactory);
        final Object[] sinks = result.columns();
        return new Results(
                (long[]) sinks[0],
                (long[]) sinks[1],
                (long[]) sinks[2],
                (long[]) sinks[3],
                (double[]) sinks[4],
                (double[]) sinks[5],
                (double[]) sinks[6],
                (double[]) sinks[7]);
    }
}
