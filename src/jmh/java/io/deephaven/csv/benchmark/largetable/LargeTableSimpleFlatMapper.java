package io.deephaven.csv.benchmark.largetable;

import io.deephaven.csv.benchmark.util.CustomDoubleParserLoadMemoized;
import io.deephaven.csv.benchmark.util.DateTimeToLongParser;
import io.deephaven.csv.benchmark.util.DateTimeToLongParser.Deephaven;
import org.simpleflatmapper.lightningcsv.CsvParser;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class LargeTableSimpleFlatMapper {
    public static Results read(final InputStream in, final Results results) throws Exception {
        final DateTimeToLongParser dtlp = new Deephaven();

        Iterator<String[]> iterator = CsvParser.iterator(new InputStreamReader(in, StandardCharsets.UTF_8));
        // Skip header row
        if (iterator.hasNext()) {
            iterator.next();
        }
        int row = 0;
        while (iterator.hasNext()) {
            final String[] next = iterator.next();
            results.timestamps[row] = dtlp.parse(next[0]);
            results.strings[row] = next[1];
            results.boolsAsBytes[row] = Boolean.parseBoolean(next[2]) ? (byte) 1 : (byte) 0;
            results.longs0[row] = Long.parseLong(next[3]);
            results.longs1[row] = Long.parseLong(next[4]);
            results.doubles0[row] = CustomDoubleParserLoadMemoized.parseDouble(next[5]);
            results.doubles1[row] = CustomDoubleParserLoadMemoized.parseDouble(next[6]);
            results.doubles2[row] = CustomDoubleParserLoadMemoized.parseDouble(next[7]);
            ++row;
            if ((row % 1_000_000) == 0) {
                System.out.printf("SimpleFlatMapper: processed %d rows\n", row);
            }
        }
        return results;
    }
}
