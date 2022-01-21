package io.deephaven.csvbench;

import gnu.trove.list.array.TDoubleArrayList;
import org.assertj.core.api.Assertions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class BenchmarkDoubles implements KosakianBenchmark {
    private static final int NUM_COLS = 3;
    private static final int NUM_ROWS = 5_000_000;
    private TableTextAndData tableTextAndData;
    private ByteArrayInputStream tableTextStream;
    private double[][] expectedResult;
    private double[][] actualResult;

    public void setup() {
        final Random rng = new Random(12345);
        tableTextAndData = makeTable(rng, NUM_ROWS, NUM_COLS);
        tableTextStream = new ByteArrayInputStream(tableTextAndData.text().getBytes(StandardCharsets.UTF_8));
        expectedResult = new double[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            expectedResult[ii] = (double[]) tableTextAndData.columns()[ii];
        }
    }

    public void checkResult() {
        Assertions.assertThat(actualResult).isEqualTo(expectedResult);
    }

    public void teardown() {}

    public static TableTextAndData makeTable(final Random rng, final int numRows, final int numCols) {
        final List<ColumnTextAndData<?>> tvs = new ArrayList<>();
        for (int ii = 0; ii < numCols; ++ii) {
            tvs.add(makeDoubleColumn(rng, numRows));
        }
        return TableTextAndData.of(tvs);
    }

    private static ColumnTextAndData<double[]> makeDoubleColumn(Random rng, final int numRows) {
        final String[] text = new String[numRows];
        final double[] data = new double[numRows];
        for (int ii = 0; ii < numRows; ++ii) {
            final double nextValue = rng.nextDouble();
            data[ii] = nextValue;
            text[ii] = Double.toString(nextValue);
        }
        return new ColumnTextAndData<>(text, data);
    }

    public void deephaven() throws io.deephaven.csv.util.CsvReaderException {
        final io.deephaven.csv.CsvSpecs specs = io.deephaven.csv.CsvSpecs.csv();
        final io.deephaven.csv.sinks.SinkFactory sf = MySinkFactory.create();
        final io.deephaven.csv.reading.CsvReader.Result result =
                io.deephaven.csv.reading.CsvReader.read(specs, tableTextStream, sf);

        actualResult = new double[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            MySinkFactory.ResultProvider<?> rp = (MySinkFactory.ResultProvider<?>) result.columns()[ii];
            actualResult[ii] = (double[]) rp.toResult();
        }
    }

    public void apacheCommons() throws IOException {
        final org.apache.commons.csv.CSVFormat format = org.apache.commons.csv.CSVFormat.DEFAULT
                .builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setRecordSeparator('\n')
                .build();

        final org.apache.commons.csv.CSVParser parser =
                new org.apache.commons.csv.CSVParser(new StringReader(tableTextAndData.text()), format);

        final TDoubleArrayList[] results = new TDoubleArrayList[NUM_COLS];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results[ii] = new TDoubleArrayList();
        }
        for (org.apache.commons.csv.CSVRecord next : parser) {
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results[ii].add(Double.parseDouble(next.get(ii)));
            }
        }
        actualResult = new double[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            actualResult[ii] = results[ii].toArray();
        }
    }

    public void fastCsv() {
        final de.siegmar.fastcsv.reader.CloseableIterator<de.siegmar.fastcsv.reader.CsvRow> iterator =
                de.siegmar.fastcsv.reader.CsvReader.builder()
                        .build(tableTextAndData.text())
                        .iterator();

        // Skip header row
        if (iterator.hasNext()) {
            iterator.next();
        }
        final TDoubleArrayList[] results = new TDoubleArrayList[NUM_COLS];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results[ii] = new TDoubleArrayList();
        }
        while (iterator.hasNext()) {
            final de.siegmar.fastcsv.reader.CsvRow next = iterator.next();
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results[ii].add(Double.parseDouble(next.getField(ii)));
            }
        }
        actualResult = new double[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            actualResult[ii] = results[ii].toArray();
        }
    }

    public void jacksonCsv() throws IOException {
        final com.fasterxml.jackson.databind.MappingIterator<List<String>> iterator =
                new com.fasterxml.jackson.dataformat.csv.CsvMapper()
                        .enable(com.fasterxml.jackson.dataformat.csv.CsvParser.Feature.WRAP_AS_ARRAY)
                        .readerFor(List.class)
                        .readValues(tableTextAndData.text());

        // Skip header row
        if (iterator.hasNext()) {
            iterator.next();
        }
        final TDoubleArrayList[] results = new TDoubleArrayList[NUM_COLS];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results[ii] = new TDoubleArrayList();
        }
        while (iterator.hasNext()) {
            final List<String> next = iterator.next();
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results[ii].add(Double.parseDouble(next.get(ii)));
            }
        }
        actualResult = new double[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            actualResult[ii] = results[ii].toArray();
        }
    }

    public void openCsv() throws IOException, com.opencsv.exceptions.CsvValidationException {
        final com.opencsv.CSVReader csvReader = new com.opencsv.CSVReader(new StringReader(tableTextAndData.text()));
        if (csvReader.readNext() == null) {
            throw new RuntimeException("Expected header row");
        }
        final TDoubleArrayList[] results = new TDoubleArrayList[NUM_COLS];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results[ii] = new TDoubleArrayList();
        }
        while (true) {
            final String[] next = csvReader.readNext();
            if (next == null) {
                break;
            }
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results[ii].add(Double.parseDouble(next[ii]));
            }
        }
        actualResult = new double[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            actualResult[ii] = results[ii].toArray();
        }
    }

    public void simpleFlatMapper() throws IOException {
        Iterator<String[]> iterator = org.simpleflatmapper.lightningcsv.CsvParser.iterator(tableTextAndData.text());
        // Skip header row
        if (iterator.hasNext()) {
            iterator.next();
        }

        final TDoubleArrayList[] results = new TDoubleArrayList[NUM_COLS];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results[ii] = new TDoubleArrayList();
        }
        while (iterator.hasNext()) {
            final String[] next = iterator.next();
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results[ii].add(Double.parseDouble(next[ii]));
            }
        }
        actualResult = new double[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            actualResult[ii] = results[ii].toArray();
        }
    }

    public void superCsv() throws IOException {
        final org.supercsv.io.CsvListReader csvReader =
                new org.supercsv.io.CsvListReader(new StringReader(tableTextAndData.text()),
                        org.supercsv.prefs.CsvPreference.STANDARD_PREFERENCE);
        if (csvReader.read() == null) {
            throw new RuntimeException("Expected header row");
        }
        final TDoubleArrayList[] results = new TDoubleArrayList[NUM_COLS];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results[ii] = new TDoubleArrayList();
        }
        while (true) {
            final List<String> next = csvReader.read();
            if (next == null) {
                break;
            }
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results[ii].add(Double.parseDouble(next.get(ii)));
            }
        }
        actualResult = new double[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            actualResult[ii] = results[ii].toArray();
        }
    }

    public void univocity() {
        final com.univocity.parsers.csv.CsvParserSettings settings = new com.univocity.parsers.csv.CsvParserSettings();
        settings.setNullValue("");
        final com.univocity.parsers.csv.CsvParser parser = new com.univocity.parsers.csv.CsvParser(settings);
        parser.beginParsing(new StringReader(tableTextAndData.text()));

        if (parser.parseNext() == null) {
            throw new RuntimeException("Expected header row");
        }
        final TDoubleArrayList[] results = new TDoubleArrayList[NUM_COLS];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results[ii] = new TDoubleArrayList();
        }
        while (true) {
            final String[] next = parser.parseNext();
            if (next == null) {
                break;
            }
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results[ii].add(Double.parseDouble(next[ii]));
            }
        }
        actualResult = new double[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            actualResult[ii] = results[ii].toArray();
        }
    }
}
