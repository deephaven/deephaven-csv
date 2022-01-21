package io.deephaven.csvbench;

import org.assertj.core.api.Assertions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class BenchmarkStrings implements KosakianBenchmark {
    private static final int NUM_COLS = 3;
    private static final int NUM_ROWS = 1_000_000;
    private static final int STRING_LENGTH = 50;
    private TableTextAndData tableTextAndData;
    private ByteArrayInputStream tableTextStream;
    private String[][] expectedResult;
    private String[][] actualResult;

    public void setup() {
        final Random rng = new Random(12345);
        tableTextAndData = makeTable(rng, NUM_ROWS, NUM_COLS);
        tableTextStream = new ByteArrayInputStream(tableTextAndData.text().getBytes(StandardCharsets.UTF_8));
        expectedResult = new String[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            expectedResult[ii] = (String[]) tableTextAndData.columns()[ii];
        }
    }

    public void checkResult() {
        Assertions.assertThat(actualResult).isEqualTo(expectedResult);
    }

    public void teardown() {}

    public static TableTextAndData makeTable(final Random rng, final int numRows, final int numCols) {
        final List<ColumnTextAndData<?>> tvs = new ArrayList<>();
        for (int ii = 0; ii < numCols; ++ii) {
            tvs.add(makeStringColumn(rng, numRows));
        }
        return TableTextAndData.of(tvs);
    }

    private static ColumnTextAndData<String[]> makeStringColumn(Random rng, final int numRows) {
        final String[] text = new String[numRows];
        final StringBuilder sb = new StringBuilder();
        for (int ii = 0; ii < numRows; ++ii) {
            sb.setLength(0);
            for (int c = 0; c < STRING_LENGTH; ++c) {
                sb.append((char) ('a' + rng.nextInt(26)));
            }
            text[ii] = sb.toString();
        }
        return new ColumnTextAndData<>(text, text);
    }

    public void deephaven() throws io.deephaven.csv.util.CsvReaderException {
        final io.deephaven.csv.CsvSpecs specs = io.deephaven.csv.CsvSpecs.csv();
        final io.deephaven.csv.sinks.SinkFactory sf = MySinkFactory.create();
        final io.deephaven.csv.reading.CsvReader.Result result =
                io.deephaven.csv.reading.CsvReader.read(specs, tableTextStream, sf);

        actualResult = new String[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            MySinkFactory.ResultProvider<?> rp = (MySinkFactory.ResultProvider<?>) result.columns()[ii];
            actualResult[ii] = (String[]) rp.toResult();
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

        final List<List<String>> results = new ArrayList<>();
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results.add(new ArrayList<>());
        }
        for (org.apache.commons.csv.CSVRecord next : parser) {
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results.get(ii).add(next.get(ii));
            }
        }
        actualResult = new String[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            actualResult[ii] = results.get(ii).toArray(new String[0]);
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
        final List<List<String>> results = new ArrayList<>();
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results.add(new ArrayList<>());
        }
        while (iterator.hasNext()) {
            final de.siegmar.fastcsv.reader.CsvRow next = iterator.next();
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results.get(ii).add(next.getField(ii));
            }
        }
        actualResult = new String[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            actualResult[ii] = results.get(ii).toArray(new String[0]);
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
        final List<List<String>> results = new ArrayList<>();
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results.add(new ArrayList<>());
        }
        while (iterator.hasNext()) {
            final List<String> next = iterator.next();
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results.get(ii).add(next.get(ii));
            }
        }
        actualResult = new String[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            actualResult[ii] = results.get(ii).toArray(new String[0]);
        }
    }

    public void openCsv() throws IOException, com.opencsv.exceptions.CsvValidationException {
        final com.opencsv.CSVReader csvReader = new com.opencsv.CSVReader(new StringReader(tableTextAndData.text()));
        if (csvReader.readNext() == null) {
            throw new RuntimeException("Expected header row");
        }
        final List<List<String>> results = new ArrayList<>();
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results.add(new ArrayList<>());
        }
        while (true) {
            final String[] next = csvReader.readNext();
            if (next == null) {
                break;
            }
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results.get(ii).add(next[ii]);
            }
        }
        actualResult = new String[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            actualResult[ii] = results.get(ii).toArray(new String[0]);
        }
    }

    public void simpleFlatMapper() throws IOException {
        Iterator<String[]> iterator = org.simpleflatmapper.lightningcsv.CsvParser.iterator(tableTextAndData.text());
        // Skip header row
        if (iterator.hasNext()) {
            iterator.next();
        }

        final List<List<String>> results = new ArrayList<>();
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results.add(new ArrayList<>());
        }
        while (iterator.hasNext()) {
            final String[] next = iterator.next();
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results.get(ii).add(next[ii]);
            }
        }
        actualResult = new String[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            actualResult[ii] = results.get(ii).toArray(new String[0]);
        }

    }

    public void superCsv() throws IOException {
        final org.supercsv.io.CsvListReader csvReader =
                new org.supercsv.io.CsvListReader(new StringReader(tableTextAndData.text()),
                        org.supercsv.prefs.CsvPreference.STANDARD_PREFERENCE);
        if (csvReader.read() == null) {
            throw new RuntimeException("Expected header row");
        }

        final List<List<String>> results = new ArrayList<>();
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results.add(new ArrayList<>());
        }
        while (true) {
            final List<String> next = csvReader.read();
            if (next == null) {
                break;
            }
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results.get(ii).add(next.get(ii));
            }
        }
        actualResult = new String[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            actualResult[ii] = results.get(ii).toArray(new String[0]);
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

        final List<List<String>> results = new ArrayList<>();
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results.add(new ArrayList<>());
        }
        while (true) {
            final String[] next = parser.parseNext();
            if (next == null) {
                break;
            }
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results.get(ii).add(next[ii]);
            }
        }
        actualResult = new String[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            actualResult[ii] = results.get(ii).toArray(new String[0]);
        }
    }
}
