package io.deephaven.csvbench;

import gnu.trove.list.array.TLongArrayList;
import org.assertj.core.api.Assertions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class BenchmarkDateTimes implements KosakianBenchmark {
    private static final int NUM_COLS = 3;
    private static final int NUM_ROWS = 500_000;
    private TableTextAndData tableTextAndData;
    private ByteArrayInputStream tableTextStream;
    private long[][] expectedResult;
    private long[][] actualResult;

    public void setup() {
        final Random rng = new Random(12345);
        tableTextAndData = makeTable(rng, NUM_ROWS, NUM_COLS);
        tableTextStream = new ByteArrayInputStream(tableTextAndData.text().getBytes(StandardCharsets.UTF_8));
        expectedResult = new long[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            expectedResult[ii] = (long[]) tableTextAndData.columns()[ii];
        }
    }

    public void checkResult() {
        Assertions.assertThat(actualResult).isEqualTo(expectedResult);
    }

    public void teardown() {}

    public static TableTextAndData makeTable(final Random rng, final int numRows, final int numCols) {
        final List<ColumnTextAndData<?>> tvs = new ArrayList<>();
        for (int ii = 0; ii < numCols; ++ii) {
            tvs.add(makeDateTimeColumn(rng, numRows));
        }
        return TableTextAndData.of(tvs);
    }

    private static ColumnTextAndData<long[]> makeDateTimeColumn(Random rng, final int numRows) {
        final String[] text = new String[numRows];
        final long[] data = new long[numRows];
        for (int ii = 0; ii < numRows; ++ii) {
            final int yyyy = 2000 + rng.nextInt(1000);
            final int MM = 1 + rng.nextInt(12);
            final int dd = 1 + rng.nextInt(28);
            final int hh = rng.nextInt(24);
            final int mm = rng.nextInt(60);
            final int ss = rng.nextInt(60);
            final int nanos = rng.nextInt(1_000_000_000);
            final ZonedDateTime zdt = ZonedDateTime.of(yyyy, MM, dd, hh, mm, ss, nanos, ZoneOffset.UTC);
            final long zdtSeconds = zdt.toEpochSecond();
            final int zdtNanos = zdt.getNano();
            data[ii] = zdtSeconds * 1_000_000_000 + zdtNanos;
            text[ii] = zdt.toString();
        }
        return new ColumnTextAndData<>(text, data);
    }

    public void deephaven() throws io.deephaven.csv.util.CsvReaderException {
        final io.deephaven.csv.reading.CsvReader reader = new io.deephaven.csv.reading.CsvReader();
        final io.deephaven.csv.sinks.SinkFactory sf = MySinkFactory.create();
        final io.deephaven.csv.reading.CsvReader.Result result = reader.read(tableTextStream, sf);

        actualResult = new long[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            MySinkFactory.ResultProvider<?> rp = (MySinkFactory.ResultProvider<?>) result.columns()[ii];
            actualResult[ii] = (long[]) rp.toResult();
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

        final TLongArrayList[] results = new TLongArrayList[NUM_COLS];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results[ii] = new TLongArrayList();
        }
        for (org.apache.commons.csv.CSVRecord next : parser) {
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results[ii].add(parseDateTime(next.get(ii)));
            }
        }
        actualResult = new long[NUM_COLS][];
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
        final TLongArrayList[] results = new TLongArrayList[NUM_COLS];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results[ii] = new TLongArrayList();
        }
        while (iterator.hasNext()) {
            final de.siegmar.fastcsv.reader.CsvRow next = iterator.next();
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results[ii].add(parseDateTime(next.getField(ii)));
            }
        }
        actualResult = new long[NUM_COLS][];
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
        final TLongArrayList[] results = new TLongArrayList[NUM_COLS];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results[ii] = new TLongArrayList();
        }
        while (iterator.hasNext()) {
            final List<String> next = iterator.next();
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results[ii].add(parseDateTime(next.get(ii)));
            }
        }
        actualResult = new long[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            actualResult[ii] = results[ii].toArray();
        }
    }

    public void openCsv() throws IOException, com.opencsv.exceptions.CsvValidationException {
        final com.opencsv.CSVReader csvReader = new com.opencsv.CSVReader(new StringReader(tableTextAndData.text()));
        if (csvReader.readNext() == null) {
            throw new RuntimeException("Expected header row");
        }
        final TLongArrayList[] results = new TLongArrayList[NUM_COLS];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results[ii] = new TLongArrayList();
        }
        while (true) {
            final String[] next = csvReader.readNext();
            if (next == null) {
                break;
            }
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results[ii].add(parseDateTime(next[ii]));
            }
        }
        actualResult = new long[NUM_COLS][];
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

        final TLongArrayList[] results = new TLongArrayList[NUM_COLS];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results[ii] = new TLongArrayList();
        }
        while (iterator.hasNext()) {
            final String[] next = iterator.next();
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results[ii].add(parseDateTime(next[ii]));
            }
        }
        actualResult = new long[NUM_COLS][];
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

        final TLongArrayList[] results = new TLongArrayList[NUM_COLS];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results[ii] = new TLongArrayList();
        }
        while (true) {
            final List<String> next = csvReader.read();
            if (next == null) {
                break;
            }
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results[ii].add(parseDateTime(next.get(ii)));
            }
        }
        actualResult = new long[NUM_COLS][];
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

        final TLongArrayList[] results = new TLongArrayList[NUM_COLS];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            results[ii] = new TLongArrayList();
        }
        while (true) {
            final String[] next = parser.parseNext();
            if (next == null) {
                break;
            }
            for (int ii = 0; ii < NUM_COLS; ++ii) {
                results[ii].add(parseDateTime(next[ii]));
            }
        }
        actualResult = new long[NUM_COLS][];
        for (int ii = 0; ii < NUM_COLS; ++ii) {
            actualResult[ii] = results[ii].toArray();
        }
    }

    private static long parseDateTime(final String dateText) {
        final ZonedDateTime zdt = ZonedDateTime.parse(dateText);
        final long zdtSeconds = zdt.toEpochSecond();
        final int zdtNanos = zdt.getNano();
        return zdtSeconds * 1_000_000_000 + zdtNanos;
    }
}
