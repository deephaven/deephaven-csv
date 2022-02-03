package io.deephaven.csv.benchmark.doublecol;

import de.siegmar.fastcsv.reader.CloseableIterator;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;
import io.deephaven.csv.benchmark.util.BenchmarkResult;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.function.ToDoubleFunction;

public final class DoubleColumnParserFastCsv {
    public static BenchmarkResult<double[]> read(final InputStream in, final double[][] storage,
            ToDoubleFunction<String> doubleParser) throws Exception {

        final CloseableIterator<CsvRow> iterator =
                CsvReader.builder().build(new InputStreamReader(in, StandardCharsets.UTF_8)).iterator();
        // Skip header row
        if (iterator.hasNext()) {
            iterator.next();
        }
        int row = 0;
        while (iterator.hasNext()) {
            final CsvRow next = iterator.next();
            for (int col = 0; col < next.getFieldCount(); ++col) {
                storage[col][row] = doubleParser.applyAsDouble(next.getField(col));
            }
            ++row;
        }
        return BenchmarkResult.of(row, storage);
    }
}
