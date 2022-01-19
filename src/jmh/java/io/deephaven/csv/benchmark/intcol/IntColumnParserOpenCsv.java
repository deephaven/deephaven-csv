package io.deephaven.csv.benchmark.intcol;

import com.opencsv.CSVReader;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public final class IntColumnParserOpenCsv extends IntColumnParserRowOriented {
    @Override
    public void readInto(InputStream in, Collector collector) throws Exception {
        final CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        if (csvReader.readNext() == null) {
            throw new RuntimeException("Expected header row");
        }
        while (true) {
            final String[] next = csvReader.readNext();
            if (next == null) {
                break;
            }
            collector.acceptRow(Integer.parseInt(next[0]));
        }
    }
}
