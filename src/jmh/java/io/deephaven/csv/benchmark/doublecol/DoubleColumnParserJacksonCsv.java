package io.deephaven.csv.benchmark.doublecol;

import ch.randelshofer.fastdoubleparser.FastDoubleParser;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.deephaven.csv.benchmark.util.BenchmarkResult;

import java.io.InputStream;
import java.util.List;

public final class DoubleColumnParserJacksonCsv {
    public static class Row {
        @JsonProperty
        public double Col1;
        @JsonProperty
        public double Col2;
        @JsonProperty
        public double Col3;
        @JsonProperty
        public double Col4;
        @JsonProperty
        public double Col5;
    }

    public static BenchmarkResult<double[]> read(final InputStream in, final String[] headers,
            final double[][] storage)
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

    public static class FDPRow {
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

    public static BenchmarkResult<double[]> readFDP(final InputStream in, final String[] headers,
            final double[][] storage)
            throws Exception {
        if (headers.length != 5) {
            throw new RuntimeException("JacksonCsv benchmark has been special-cased to assume 5 columns");
        }
        final CsvSchema.Builder builder = CsvSchema.builder();
        for (String header : headers) {
            builder.addColumn(header);
        }
        final CsvSchema schema = builder.build().withSkipFirstDataRow(true);
        final MappingIterator<FDPRow> it = new CsvMapper()
                .readerFor(FDPRow.class)
                .with(schema)
                .readValues(in);
        int rowNum = 0;
        while (it.hasNext()) {
            final FDPRow row = it.next();
            storage[0][rowNum] = FastDoubleParser.parseDouble(row.Col1);
            storage[1][rowNum] = FastDoubleParser.parseDouble(row.Col2);
            storage[2][rowNum] = FastDoubleParser.parseDouble(row.Col3);
            storage[3][rowNum] = FastDoubleParser.parseDouble(row.Col4);
            storage[4][rowNum] = FastDoubleParser.parseDouble(row.Col5);
            ++rowNum;
        }
        return BenchmarkResult.of(rowNum, storage);
    }
}
