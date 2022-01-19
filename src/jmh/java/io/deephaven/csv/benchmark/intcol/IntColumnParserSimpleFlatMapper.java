package io.deephaven.csv.benchmark.intcol;

import org.simpleflatmapper.lightningcsv.CsvParser;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public final class IntColumnParserSimpleFlatMapper extends IntColumnParserRowOriented {

    @Override
    public void readInto(InputStream in, Collector collector) throws Exception {
        Iterator<String[]> iterator = CsvParser.iterator(new InputStreamReader(in, StandardCharsets.UTF_8));
        // Skip header row
        if (iterator.hasNext()) {
            iterator.next();
        }
        while (iterator.hasNext()) {
            final String[] next = iterator.next();
            collector.acceptRow(Integer.parseInt(next[0]));
        }
    }
}
