package io.deephaven.csv.benchmark.largetable;

import ch.randelshofer.fastdoubleparser.FastDoubleParser;
import io.deephaven.csv.benchmark.util.DateTimeToLongParser;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class LargeTableSuperCsv {
    public static Results read(final InputStream in, final Results results) throws Exception {
        final DateTimeToLongParser dtlp = new DateTimeToLongParser.Deephaven();

        final CsvListReader csvReader =
                new CsvListReader(new InputStreamReader(in, StandardCharsets.UTF_8), CsvPreference.STANDARD_PREFERENCE);
        if (csvReader.read() == null) {
            throw new RuntimeException("Expected header row");
        }
        int row = 0;
        while (true) {
            final List<String> next = csvReader.read();
            if (next == null) {
                break;
            }

            results.timestamps[row] = dtlp.parse(next.get(0));
            results.strings[row] = next.get(1);
            results.boolsAsBytes[row] = Boolean.parseBoolean(next.get(2)) ? (byte) 1 : (byte) 0;
            results.longs0[row] = Long.parseLong(next.get(3));
            results.longs1[row] = Long.parseLong(next.get(4));
            results.doubles0[row] = FastDoubleParser.parseDouble(next.get(5));
            results.doubles1[row] = FastDoubleParser.parseDouble(next.get(6));
            results.doubles2[row] = FastDoubleParser.parseDouble(next.get(7));
            ++row;
            if ((row % 1_000_000) == 0) {
                System.out.printf("SuperCsv: processed %d rows\n", row);
            }
        }
        return results;
    }
}
