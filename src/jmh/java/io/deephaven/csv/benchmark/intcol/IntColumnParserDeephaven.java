package io.deephaven.csv.benchmark.intcol;

import io.deephaven.csv.parsers.IntParser;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csvbench.MySinkFactory;
import io.deephaven.csvbench.MySinkFactory.ResultProvider;

import java.io.InputStream;

public final class IntColumnParserDeephaven implements IntColumnParser {

    @Override
    public ResultProvider<int[]> read(InputStream in) throws Exception {
        final SinkFactory sinkFactory = MySinkFactory.create();
        final CsvReader reader = new CsvReader()
                .setParserFor(0, IntParser.INSTANCE)
                .setHasHeaders(true)
                .setHeaders("X");
        final CsvReader.Result result = reader.read(in, sinkFactory);
        // noinspection unchecked
        return (ResultProvider<int[]>) result.columns()[0];
    }
}
