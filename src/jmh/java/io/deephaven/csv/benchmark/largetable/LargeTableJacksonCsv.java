package io.deephaven.csv.benchmark.largetable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.deephaven.csv.benchmark.util.DateTimeToLongParser;

import java.io.InputStream;

public class LargeTableJacksonCsv {
    public static class Row {
        @JsonProperty("Timestamp")
        public String timestampAsString;
        @JsonProperty("String")
        public String string;
        @JsonProperty("Bool")
        public boolean bool;
        @JsonProperty("Long0")
        public long long0;
        @JsonProperty("Long1")
        public long long1;
        @JsonProperty("Double0")
        public double double0;
        @JsonProperty("Double1")
        public double double1;
        @JsonProperty("Double2")
        public double double2;
    }

    public static Results read(final InputStream in, final String[] headers, final Results results) throws Exception {
        final DateTimeToLongParser dtlp = new DateTimeToLongParser.Deephaven();

        final CsvSchema.Builder builder = CsvSchema.builder();
        for (String header : headers) {
            builder.addColumn(header);
        }
        final CsvSchema schema = builder.build().withSkipFirstDataRow(true);
        final MappingIterator<Row> it = new CsvMapper()
                .readerFor(Row.class)
                .with(schema)
                .readValues(in);
        int row = 0;
        while (it.hasNext()) {
            final Row record = it.next();
            results.timestamps[row] = dtlp.parse(record.timestampAsString);
            results.strings[row] = record.string;
            results.boolsAsBytes[row] = record.bool ? (byte) 1 : (byte) 0;
            results.longs0[row] = record.long0;
            results.longs1[row] = record.long1;
            results.doubles0[row] = record.double0;
            results.doubles1[row] = record.double1;
            results.doubles2[row] = record.double2;
            ++row;
            if ((row % 1_000_000) == 0) {
                System.out.printf("JacksonCSV: processed %d rows\n", row);
            }
        }
        return results;
    }
}
