package io.deephaven.csv.benchmark.intcol;

import de.siegmar.fastcsv.reader.CloseableIterator;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public final class IntColumnParserFastCsv extends IntColumnParserRowOriented {

    @Override
    public void readInto(InputStream in, Collector collector) throws Exception {
        final CloseableIterator<CsvRow> iterator =
                CsvReader.builder().build(new InputStreamReader(in, StandardCharsets.UTF_8)).iterator();
        // Skip header row
        if (iterator.hasNext()) {
            iterator.next();
        }
        while (iterator.hasNext()) {
            final CsvRow next = iterator.next();
            collector.acceptRow(Integer.parseInt(next.getField(0)));
        }
    }
}
