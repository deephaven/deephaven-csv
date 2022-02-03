package io.deephaven.csv.benchmark.doublecol;

import com.opencsv.CSVReader;
import io.deephaven.csv.benchmark.util.BenchmarkResult;
import io.deephaven.csv.benchmark.util.Util;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.function.ToDoubleFunction;

public final class DoubleColumnParserOpenCsv {
    public static BenchmarkResult<double[]> read(final InputStream in, final double[][] storage,
            ToDoubleFunction<String> doubleParser) throws Exception {
        final CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        if (csvReader.readNext() == null) {
            throw new RuntimeException("Expected header row");
        }
        int row = 0;
        while (true) {
            final String[] next = csvReader.readNext();
            if (next == null) {
                break;
            }
            for (int col = 0; col < next.length; ++col) {
                storage[col][row] = doubleParser.applyAsDouble(next[col]);
            }
            ++row;
        }
        return BenchmarkResult.of(row, storage);
    }
}
