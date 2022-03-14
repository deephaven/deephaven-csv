package io.deephaven.csv.benchmark.largetable;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.benchmark.util.ArrayBacked;
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
        final Sink<?>[] sinks = result.columns();
        return new Results(
                ((ArrayBacked<long[]>) sinks[0]).getUnderlyingArray(),
                ((ArrayBacked<String[]>) sinks[1]).getUnderlyingArray(),
                ((ArrayBacked<byte[]>) sinks[2]).getUnderlyingArray(),
                ((ArrayBacked<long[]>) sinks[3]).getUnderlyingArray(),
                ((ArrayBacked<long[]>) sinks[4]).getUnderlyingArray(),
                ((ArrayBacked<double[]>) sinks[5]).getUnderlyingArray(),
                ((ArrayBacked<double[]>) sinks[6]).getUnderlyingArray(),
                ((ArrayBacked<double[]>) sinks[7]).getUnderlyingArray());
    }
}
