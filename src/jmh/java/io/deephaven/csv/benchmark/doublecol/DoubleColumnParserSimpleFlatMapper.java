package io.deephaven.csv.benchmark.doublecol;

import io.deephaven.csv.benchmark.util.CustomDoubleParserLoadMemoized;
import io.deephaven.csv.benchmark.util.BenchmarkResult;
import org.simpleflatmapper.lightningcsv.CsvParser;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public final class DoubleColumnParserSimpleFlatMapper {
    public static BenchmarkResult<double[]> read(final InputStream in, final double[][] storage) throws Exception {
        Iterator<String[]> iterator = CsvParser.iterator(new InputStreamReader(in, StandardCharsets.UTF_8));
        // Skip header row
        if (iterator.hasNext()) {
            iterator.next();
        }
        int row = 0;
        while (iterator.hasNext()) {
            final String[] next = iterator.next();
            for (int col = 0; col < next.length; ++col) {
                storage[col][row] = CustomDoubleParserLoadMemoized.parseDouble(next[col]);
            }
            ++row;
        }
        return BenchmarkResult.of(row, storage);
    }
}
