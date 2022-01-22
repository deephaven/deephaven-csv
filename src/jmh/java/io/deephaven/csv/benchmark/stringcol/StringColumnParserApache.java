package io.deephaven.csv.benchmark.stringcol;

import io.deephaven.csv.benchmark.util.BenchmarkResult;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public final class StringColumnParserApache {
    public static BenchmarkResult<String[]> read(final InputStream in, final String[][] storage) throws Exception {
        final CSVFormat format = CSVFormat.DEFAULT
                .builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setRecordSeparator('\n')
                .build();
        final CSVParser parser = new CSVParser(new InputStreamReader(in, StandardCharsets.UTF_8), format);
        int row = 0;
        for (CSVRecord record : parser) {
            for (int col = 0; col < record.size(); ++col) {
                storage[col][row] = record.get(col);
            }
            ++row;
        }
        return BenchmarkResult.of(row, storage);
    }
}
