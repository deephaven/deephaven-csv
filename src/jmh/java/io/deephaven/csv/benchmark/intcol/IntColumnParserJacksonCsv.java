package io.deephaven.csv.benchmark.intcol;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.InputStream;

public final class IntColumnParserJacksonCsv extends IntColumnParserRowOriented {

    public static class Row {
        @JsonProperty
        public int X;
    }

    @Override
    public void readInto(InputStream in, Collector collector) throws Exception {
        final CsvSchema schema = CsvSchema.builder().addColumn("X").build().withSkipFirstDataRow(true);
        final MappingIterator<Row> it = new CsvMapper()
                .readerFor(Row.class)
                .with(schema)
                .readValues(in);
        while (it.hasNext()) {
            final Row row = it.next();
            collector.acceptRow(row.X);
        }
    }
}
