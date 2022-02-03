package io.deephaven.csv.benchmark.stringcol;

import io.deephaven.csv.benchmark.util.BenchmarkResult;
import org.simpleflatmapper.lightningcsv.CsvParser;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public final class StringColumnParserSimpleFlatMapper {
    public static BenchmarkResult<String[]> read(final InputStream in, final String[][] storage) throws Exception {
        Iterator<String[]> iterator = CsvParser.iterator(new InputStreamReader(in, StandardCharsets.UTF_8));
        // Skip header row
        if (iterator.hasNext()) {
            iterator.next();
        }
        int row = 0;
        while (iterator.hasNext()) {
            final String[] next = iterator.next();
            for (int col = 0; col < next.length; ++col) {
                storage[col][row] = next[col];
            }
            ++row;
        }
        return BenchmarkResult.of(row, storage);
    }
}
