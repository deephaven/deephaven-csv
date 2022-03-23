package io.deephaven.csv.benchmark.datetimecol;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.benchmark.util.BenchmarkResult;
import io.deephaven.csv.benchmark.util.SinkFactories;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.sinks.SinkFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;

public final class DateTimeColumnParserDeephaven {
    public static BenchmarkResult<long[]> read(final InputStream in, final long[][] storage) throws Exception {
        final SinkFactory sinkFactory = SinkFactories.makeRecyclingSinkFactory(null, null, null, null, null, storage);
        final CsvSpecs specs = CsvSpecs.builder()
                .parsers(Collections.singleton(Parsers.DATETIME))
                .hasHeaderRow(true)
                .build();
        final CsvReader.Result result = CsvReader.read(specs, in, sinkFactory);
        final long[][] data = Arrays.stream(result.columns())
                .map(col -> ((long[]) col)).toArray(long[][]::new);
        return BenchmarkResult.of(result.numRows(), data);
    }
}
