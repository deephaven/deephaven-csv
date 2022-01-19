package io.deephaven.csv.benchmark.intcol;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public final class IntColumnParserApache extends IntColumnParserRowOriented {
    @Override
    public void readInto(InputStream in, Collector collector) throws Exception {
        final CSVFormat format = CSVFormat.DEFAULT
                .builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setRecordSeparator('\n')
                .build();
        final CSVParser parser = new CSVParser(new InputStreamReader(in, StandardCharsets.UTF_8), format);
        for (CSVRecord record : parser) {
            collector.acceptRow(Integer.parseInt(record.get(0)));
        }
    }
}
