package io.deephaven.csv.benchmark.intcol;

import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

public final class IntColumnParserSuperCsv extends IntColumnParserRowOriented {

    @Override
    public void readInto(InputStream in, Collector collector) throws Exception {
        final CsvListReader csvReader =
                new CsvListReader(new InputStreamReader(in, StandardCharsets.UTF_8), CsvPreference.STANDARD_PREFERENCE);
        if (csvReader.read() == null) {
            throw new RuntimeException("Expected header row");
        }
        while (true) {
            final List<String> next = csvReader.read();
            if (next == null) {
                break;
            }
            collector.acceptRow(Integer.parseInt(next.get(0)));
        }
    }
}
