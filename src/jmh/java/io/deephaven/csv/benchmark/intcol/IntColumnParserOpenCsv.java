package io.deephaven.csv.benchmark.intcol;

import com.opencsv.CSVReader;
import io.deephaven.csv.benchmark.util.BenchmarkResult;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public final class IntColumnParserOpenCsv {
    public static BenchmarkResult<int[]> read(final InputStream in, final int[][] storage) throws Exception {
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
                storage[col][row] = Integer.parseInt(next[col]);
            }
            ++row;
        }
        return BenchmarkResult.of(row, storage);
    }
}
