package io.deephaven.csv.benchmark.largetable;

import ch.randelshofer.fastdoubleparser.FastDoubleParser;
import io.deephaven.csv.benchmark.util.DateTimeToLongParser;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class LargeTableApache {
    public static Results read(final InputStream in, final Results results) throws Exception {
        final DateTimeToLongParser dtlp = new DateTimeToLongParser.Deephaven();

        final CSVFormat format = CSVFormat.DEFAULT
                .builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setRecordSeparator('\n')
                .build();
        final CSVParser parser = new CSVParser(new InputStreamReader(in, StandardCharsets.UTF_8), format);

        int row = 0;
        for (CSVRecord record : parser) {
            results.timestamps[row] = dtlp.parse(record.get(0));
            results.strings[row] = record.get(1);
            results.boolsAsBytes[row] = Boolean.parseBoolean(record.get(2)) ? (byte) 1 : (byte) 0;
            results.longs0[row] = Long.parseLong(record.get(3));
            results.longs1[row] = Long.parseLong(record.get(4));
            results.doubles0[row] = FastDoubleParser.parseDouble(record.get(5));
            results.doubles1[row] = FastDoubleParser.parseDouble(record.get(6));
            results.doubles2[row] = FastDoubleParser.parseDouble(record.get(7));
            ++row;
            if ((row % 1_000_000) == 0) {
                System.out.printf("Apache: processed %d rows\n", row);
            }
        }
        return results;
    }
}
