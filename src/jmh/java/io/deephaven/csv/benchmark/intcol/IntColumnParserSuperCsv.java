package io.deephaven.csv.benchmark.intcol;

import io.deephaven.csv.benchmark.util.BenchmarkResult;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

public final class IntColumnParserSuperCsv {
    public static BenchmarkResult<int[]> read(final InputStream in, final int[][] storage) throws Exception {
        final CsvListReader csvReader =
                new CsvListReader(new InputStreamReader(in, StandardCharsets.UTF_8), CsvPreference.STANDARD_PREFERENCE);
        if (csvReader.read() == null) {
            throw new RuntimeException("Expected header row");
        }
        int row = 0;
        while (true) {
            final List<String> next = csvReader.read();
            if (next == null) {
                break;
            }

            for (int col = 0; col < next.size(); ++col) {
                storage[col][row] = Integer.parseInt(next.get(col));
            }
            ++row;
        }
        return BenchmarkResult.of(row, storage);
    }
}
