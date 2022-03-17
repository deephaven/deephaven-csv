package io.deephaven.csv.benchmark.intcol;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.benchmark.util.ArrayBacked;
import io.deephaven.csv.benchmark.util.BenchmarkResult;
import io.deephaven.csv.benchmark.util.SinkFactories;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.sinks.SinkFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;

public final class IntColumnParserDeephaven {
    public static BenchmarkResult<int[]> read(final InputStream in, final int[][] storage) throws Exception {
        final SinkFactory sinkFactory = SinkFactories.makeRecyclingSinkFactory(null, storage, null, null, null, null);
        final CsvSpecs specs = CsvSpecs.builder()
                .parsers(Collections.singleton(Parsers.INT))
                .hasHeaderRow(true)
                .build();
        final CsvReader.Result result = CsvReader.read(specs, in, sinkFactory);
        final int[][] data = Arrays.stream(result.columns())
                .map(col -> ((ArrayBacked<int[]>) col).getUnderlyingArray()).toArray(int[][]::new);
        return BenchmarkResult.of(result.numRows(), data);
    }
}
