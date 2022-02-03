package io.deephaven.csv.benchmark.datetimecol;

import io.deephaven.csv.benchmark.util.BenchmarkResult;
import io.deephaven.csv.benchmark.util.DateTimeToLongParser;
import org.simpleflatmapper.lightningcsv.CsvParser;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public final class DateTimeColumnParserSimpleFlatMapper {
    public static BenchmarkResult<long[]> read(final InputStream in, final long[][] storage,
            DateTimeToLongParser dateTimeToLongParser) throws Exception {
        Iterator<String[]> iterator = CsvParser.iterator(new InputStreamReader(in, StandardCharsets.UTF_8));
        // Skip header row
        if (iterator.hasNext()) {
            iterator.next();
        }
        int row = 0;
        while (iterator.hasNext()) {
            final String[] next = iterator.next();
            for (int col = 0; col < next.length; ++col) {
                storage[col][row] = dateTimeToLongParser.parse(next[col]);
            }
            ++row;
        }
        return BenchmarkResult.of(row, storage);
    }
}
