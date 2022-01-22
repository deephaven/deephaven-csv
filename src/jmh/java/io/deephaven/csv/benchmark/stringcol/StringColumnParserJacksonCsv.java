package io.deephaven.csv.benchmark.stringcol;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.deephaven.csv.benchmark.doublecol.DoubleColumnParserJacksonCsv;
import io.deephaven.csv.benchmark.util.BenchmarkResult;

import java.io.InputStream;

public final class StringColumnParserJacksonCsv {
    public static class Row {
        @JsonProperty
        public String Col1;
        @JsonProperty
        public String Col2;
        @JsonProperty
        public String Col3;
        @JsonProperty
        public String Col4;
        @JsonProperty
        public String Col5;
    }

    public static BenchmarkResult<String[]> read(final InputStream in, final String[] headers, final String[][] storage)
            throws Exception {
        if (headers.length != 5) {
            throw new RuntimeException("JacksonCsv benchmark has been special-cased to assume 5 columns");
        }
        final CsvSchema.Builder builder = CsvSchema.builder();
        for (String header : headers) {
            builder.addColumn(header);
        }
        final CsvSchema schema = builder.build().withSkipFirstDataRow(true);
        final MappingIterator<Row> it = new CsvMapper()
                .readerFor(Row.class)
                .with(schema)
                .readValues(in);

        int rowNum = 0;
        while (it.hasNext()) {
            final Row row = it.next();
            storage[0][rowNum] = row.Col1;
            storage[1][rowNum] = row.Col2;
            storage[2][rowNum] = row.Col3;
            storage[3][rowNum] = row.Col4;
            storage[4][rowNum] = row.Col5;
            ++rowNum;
        }
        return BenchmarkResult.of(rowNum, storage);
    }
}
