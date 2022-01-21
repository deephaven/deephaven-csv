package io.deephaven.csv.benchmark.intcol;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.parsers.IntParser;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csvbench.MySinkFactory;
import io.deephaven.csvbench.MySinkFactory.ResultProvider;

import java.io.InputStream;
import java.util.List;

public final class IntColumnParserDeephaven implements IntColumnParser {

    @Override
    public ResultProvider<int[]> read(InputStream in) throws Exception {
        final SinkFactory sinkFactory = MySinkFactory.create();
        final CsvSpecs specs = CsvSpecs.builder()
                .putParserForIndex(0, Parsers.INT)
                .hasHeaderRow(true)
                .headers(List.of("X"))
                .build();
        final CsvReader.Result result = CsvReader.read(specs, in, sinkFactory);
        // noinspection unchecked
        return (ResultProvider<int[]>) result.columns()[0];
    }
}
