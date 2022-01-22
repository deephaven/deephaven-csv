package io.deephaven.csv.benchmark.doublecol;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.benchmark.util.ArrayBacked;
import io.deephaven.csv.benchmark.util.BenchmarkResult;
import io.deephaven.csv.benchmark.util.SinkFactories;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.sinks.SinkFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

public final class DoubleColumnParserDeephaven {
    public static BenchmarkResult<double[]> read(final InputStream in, final double[][] storage) throws Exception {
        final SinkFactory sinkFactory = SinkFactories.makeRecyclingSinkFactory(null, null, null, storage, null, null);
        final CsvSpecs specs = CsvSpecs.builder()
                .parsers(List.of(Parsers.DOUBLE))
                .hasHeaderRow(true)
                .build();
        final CsvReader.Result result = CsvReader.read(specs, in, sinkFactory);
        final double[][] data = Arrays.stream(result.columns())
                .map(col -> ((ArrayBacked<double[]>) col).getUnderlyingArray()).toArray(double[][]::new);
        return BenchmarkResult.of(result.numRows(), data);
    }
}
