package io.deephaven.csv.testutil;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.parsers.DataType;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.util.CsvReaderException;
import org.apache.commons.io.input.ReaderInputStream;
import org.assertj.core.api.Assertions;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CsvTestUtil {

    private static final List<Charset> STANDARD_CHARSETS = Collections.unmodifiableList(Arrays.asList(
            StandardCharsets.US_ASCII,
            StandardCharsets.ISO_8859_1,
            StandardCharsets.UTF_8,
            StandardCharsets.UTF_16,
            StandardCharsets.UTF_16BE,
            StandardCharsets.UTF_16LE));

    public static CsvSpecs defaultCsvSpecs() {
        return defaultCsvBuilder().build();
    }

    public static CsvSpecs.Builder defaultCsvBuilder() {
        return CsvSpecs.builder().ignoreSurroundingSpaces(true).allowMissingColumns(true);
    }

    public static void invokeTests(final CsvSpecs specs, final String input, final ColumnSet expected)
            throws CsvReaderException {
        for (final Charset charset : STANDARD_CHARSETS) {
            // Ensure we are always testing wrt UTF_8
            if (StandardCharsets.UTF_8.equals(charset) || charset.newEncoder().canEncode(input)) {
                invokeTest(specs, input, charset, expected, makeMySinkFactory(), null);
            }
        }
    }

    public static void invokeTests(CsvSpecs specs, String input, ColumnSet expected, SinkFactory sinkFactory,
            MakeCustomColumn makeCustomColumn) throws CsvReaderException {
        for (final Charset charset : STANDARD_CHARSETS) {
            // Ensure we are always testing wrt UTF_8
            if (StandardCharsets.UTF_8.equals(charset) || charset.newEncoder().canEncode(input)) {
                invokeTest(specs, input, charset, expected, sinkFactory, makeCustomColumn);
            }
        }
    }

    public static void invokeTest(final CsvSpecs specs, final String input, final Charset charset,
            final ColumnSet expected,
            final SinkFactory sinkFactory, final MakeCustomColumn makeCustomColumn)
            throws CsvReaderException {
        invokeTest(specs, toInputStream(input, charset), charset, expected, sinkFactory, makeCustomColumn);
    }

    public static void invokeTest(final CsvSpecs specs, final InputStream inputStream, final Charset charset,
            final ColumnSet expected,
            final SinkFactory sinkFactory, MakeCustomColumn makeCustomColumn)
            throws CsvReaderException {
        final CsvReader.Result result = parse(specs, inputStream, charset, sinkFactory);
        final ColumnSet actual = toColumnSet(result, makeCustomColumn);
        final String expectedToString = expected.toString();
        final String actualToString = actual.toString();
        Assertions.assertThat(actualToString).isEqualTo(expectedToString);
    }

    /**
     * Parses {@code inputStream} according to the specifications of {@code csvReader}.
     *
     * @param inputStream the input stream.
     * @return The parsed data
     * @throws CsvReaderException If any sort of failure occurs.
     */
    public static CsvReader.Result parse(final CsvSpecs specs, final InputStream inputStream, final Charset charset)
            throws CsvReaderException {
        return parse(specs, inputStream, charset, makeMySinkFactory());
    }

    /**
     * Parses {@code inputStream} according to the specifications of {@code csvReader}.
     *
     *
     * @param inputStream the input stream.
     * @return The parsed data
     * @throws CsvReaderException If any sort of failure occurs.
     */
    public static CsvReader.Result parse(final CsvSpecs specs, final InputStream inputStream, final Charset charset,
            final SinkFactory sinkFactory)
            throws CsvReaderException {
        return CsvReader.read(specs, inputStream, charset, sinkFactory);
    }

    /** Convert String to InputStream */
    public static InputStream toInputStream(final String input, final Charset charset) {
        final CharsetEncoder encoder = charset.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            return ReaderInputStream.builder().setCharSequence(input).setCharsetEncoder(encoder).get();
        } catch (IOException e) {
            throw new UncheckedIOException("Unexpected exception, should not happen with String input", e);
        }
    }

    /***
     * Converts the {@link CsvReader.Result} to a {@link ColumnSet}.
     */
    public static ColumnSet toColumnSet(final CsvReader.Result result, MakeCustomColumn makeCustomColumn) {
        final int numCols = result.numCols();
        final CsvReader.ResultColumn[] resultColumns = result.columns();

        final Column<?>[] columns = new Column[numCols];
        final int sizeAsInt = Math.toIntExact(result.numRows());

        for (int ii = 0; ii < numCols; ++ii) {
            final CsvReader.ResultColumn rc = resultColumns[ii];
            columns[ii] = makeColumn(rc.name(), rc.dataType(), rc.data(), sizeAsInt, makeCustomColumn);
        }
        return ColumnSet.of(columns);
    }

    private static Column<?> makeColumn(final String name, final DataType dataType, final Object col,
            final int size, MakeCustomColumn makeCustomColumn) {
        switch (dataType) {
            case BOOLEAN_AS_BYTE: {
                return Column.ofArray(name, col, size).reinterpret(boolean.class);
            }

            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case STRING: {
                return Column.ofArray(name, col, size);
            }

            case DATETIME_AS_LONG:
            case TIMESTAMP_AS_LONG: {
                return Column.ofArray(name, col, size).reinterpret(Instant.class);
            }

            case CUSTOM: {
                if (makeCustomColumn == null) {
                    throw new RuntimeException("Custom column not expected");
                }
                return makeCustomColumn.apply(name, col, size);
            }

            default: {
                throw new RuntimeException("Unknown case " + dataType);
            }
        }
    }

    public static SinkFactory makeMySinkFactory() {
        return SinkFactory.arrays(
                Sentinels.NULL_BYTE,
                Sentinels.NULL_SHORT,
                Sentinels.NULL_INT,
                Sentinels.NULL_LONG,
                Sentinels.NULL_FLOAT,
                Sentinels.NULL_DOUBLE,
                Sentinels.NULL_BOOLEAN_AS_BYTE,
                Sentinels.NULL_CHAR,
                null,
                Sentinels.NULL_DATETIME_AS_LONG,
                Sentinels.NULL_TIMESTAMP_AS_LONG);
    }

    public static SinkFactory makeBlackholeSinkFactory() {
        return SinkFactory.of(
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                Blackhole::new);
    }

    public static SinkFactory makeBlackholeSinkFactoryWithFailingDoubleSink() {
        return SinkFactory.of(
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                FailingSink::new,
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                Blackhole::new);
    }

    public static SinkFactory makeCooperatingSinkFactories(
            Duration timeout, CountDownLatch incomingShutdownRequest,
            CountDownLatch outgoingShutdownComplete) {
        final CountDownLatch sinkReady = new CountDownLatch(1);
        return SinkFactory.of(
                Blackhole::new,
                Blackhole::new,
                colNum -> new ThrowingSink<>(sinkReady),
                Blackhole::new,
                Blackhole::new,
                colNum -> new StubbornSink<>(timeout, sinkReady,
                        incomingShutdownRequest, outgoingShutdownComplete),
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                Blackhole::new);
    }

    public static SinkFactory makeBlackholeSinkFactoryWithSynchronizingDoubleSink(final int numParticipatingColumns,
            final long thresholdSize) {
        final SyncState ss = new SyncState(numParticipatingColumns, thresholdSize);
        return SinkFactory.of(
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                colNum -> new SynchronizingSink<>(colNum, ss),
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                Blackhole::new,
                Blackhole::new);
    }

    public static String repeat(String x, int count) {
        return Stream.generate(() -> x).limit(count).collect(Collectors.joining());
    }
}
