package io.deephaven.csv.benchmark.largetable;

import ch.randelshofer.fastdoubleparser.FastDoubleParser;
import de.siegmar.fastcsv.reader.CloseableIterator;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;
import io.deephaven.csv.benchmark.util.DateTimeToLongParser;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class LargeTableFastCsv {
    public static Results read(final InputStream in, final Results results) throws Exception {
        final DateTimeToLongParser dtlp = new DateTimeToLongParser.Deephaven();

        final CloseableIterator<CsvRow> iterator =
                CsvReader.builder().build(new InputStreamReader(in, StandardCharsets.UTF_8)).iterator();
        // Skip header row
        if (iterator.hasNext()) {
            iterator.next();
        }
        int row = 0;
        while (iterator.hasNext()) {
            final CsvRow next = iterator.next();
            results.timestamps[row] = dtlp.parse(next.getField(0));
            results.strings[row] = next.getField(1);
            results.boolsAsBytes[row] = Boolean.parseBoolean(next.getField(2)) ? (byte) 1 : (byte) 0;
            results.longs0[row] = Long.parseLong(next.getField(3));
            results.longs1[row] = Long.parseLong(next.getField(4));
            results.doubles0[row] = FastDoubleParser.parseDouble(next.getField(5));
            results.doubles1[row] = FastDoubleParser.parseDouble(next.getField(6));
            results.doubles2[row] = FastDoubleParser.parseDouble(next.getField(7));
            ++row;
            if ((row % 1_000_000) == 0) {
                System.out.printf("FastCSV: processed %d rows\n", row);
            }
        }
        return results;
    }
}
