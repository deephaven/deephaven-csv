package io.deephaven.csv.benchmark.stringcol;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.benchmark.util.BenchmarkResult;
import io.deephaven.csv.benchmark.util.SinkFactories;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.sinks.SinkFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;

public final class StringColumnParserDeephaven {
    public static BenchmarkResult<String[]> read(final InputStream in, final String[][] storage) throws Exception {
        final SinkFactory sinkFactory = SinkFactories.makeRecyclingSinkFactory(null, null, null, null, storage, null);
        final CsvSpecs specs = CsvSpecs.builder()
                .parsers(Collections.singleton(Parsers.STRING))
                .hasHeaderRow(true)
                .build();
        final CsvReader.Result result = CsvReader.read(specs, in, sinkFactory);
        final String[][] data = Arrays.stream(result.columns())
                .map(col -> ((String[]) col.data())).toArray(String[][]::new);
        return BenchmarkResult.of(result.numRows(), data);
    }
}
