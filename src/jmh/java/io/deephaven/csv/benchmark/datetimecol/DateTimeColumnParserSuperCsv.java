package io.deephaven.csv.benchmark.datetimecol;

import io.deephaven.csv.benchmark.util.BenchmarkResult;
import io.deephaven.csv.benchmark.util.DateTimeToLongParser;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

public final class DateTimeColumnParserSuperCsv {
    public static BenchmarkResult<long[]> read(final InputStream in, final long[][] storage,
            DateTimeToLongParser dateTimeToLongParser) throws Exception {
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
                storage[col][row] = dateTimeToLongParser.parse(next.get(col));
            }
            ++row;
        }
        return BenchmarkResult.of(row, storage);
    }
}
