package io.deephaven.csv;

import gnu.trove.list.array.TByteArrayList;
import gnu.trove.list.array.TCharArrayList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.array.TShortArrayList;
import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.densestorage.DenseStorageConstants;
import io.deephaven.csv.parsers.DataType;
import io.deephaven.csv.parsers.IteratorHolder;
import io.deephaven.csv.parsers.Parser;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.reading.cells.DelimitedCellGrabber;
import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.sinks.Source;
import io.deephaven.csv.tokenization.RangeTests;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.csv.util.Renderer;
import org.apache.commons.io.input.ReaderInputStream;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.*;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CsvReaderTest {
    private static class Sentinels {
        public static final byte NULL_BOOLEAN_AS_BYTE = Byte.MIN_VALUE;
        public static final byte NULL_BYTE = Byte.MIN_VALUE;
        public static final short NULL_SHORT = Short.MIN_VALUE;
        public static final int NULL_INT = Integer.MIN_VALUE;
        public static final long NULL_LONG = Long.MIN_VALUE;
        public static final float NULL_FLOAT = -Float.MAX_VALUE;
        public static final double NULL_DOUBLE = -Double.MAX_VALUE;
        public static final char NULL_CHAR = Character.MAX_VALUE;
        public static final long NULL_DATETIME_AS_LONG = Long.MIN_VALUE;
        public static final long NULL_TIMESTAMP_AS_LONG = Long.MIN_VALUE;
    }

    /**
     * Addresses <a href="https://github.com/deephaven/deephaven-core/issues/2133">Deephaven Core Issue #2133</a>.
     */
    @Test
    public void bug2133() throws CsvReaderException {
        final int bufferSize = DelimitedCellGrabber.BUFFER_SIZE;
        final StringBuilder sb = new StringBuilder("Values\r");
        final int numAs = bufferSize - sb.length() - 1;
        final String expected1 = repeat("a", numAs);
        final String expected2 = repeat("b", bufferSize);
        sb.append(expected1).append('\r').append(expected2).append('\r');
        final String input = sb.toString();
        final CsvReader.Result result = parse(defaultCsvSpecs(), toInputStream(input));
        final String[] col = (String[]) result.columns()[0].data();
        final String row1 = col[0];
        final String row2 = col[1];
        Assertions.assertThat(row1).isEqualTo(expected1);
        Assertions.assertThat(row2).isEqualTo(expected2);
    }

    /**
     * Addresses <a href="https://github.com/deephaven/deephaven-csv/issues/48">Deephaven CSV Issue #48</a>. When the
     * bug exists, the library hangs (and this tests times out). When the bug is fixed, the library instantly propagates
     * the exception and the test succeeds (that is, the library fails as expected).
     */
    @Test
    @Timeout(value = 10)
    public void bug48() {
        final int numRows = 50_000_000;
        final RepeatingInputStream inputStream = new RepeatingInputStream("Col1,Col2\n", "1,2.2\n", numRows);
        Assertions
                .assertThatThrownBy(
                        () -> invokeTest(defaultCsvSpecs(),
                                inputStream,
                                ColumnSet.NONE,
                                makeBlackholeSinkFactoryWithFailingDoubleSink(),
                                null))
                .hasRootCauseMessage("synthetic error for testing: out of memory");
    }

    /**
     * Addresses <a href="https://github.com/deephaven/deephaven-csv/issues/52">Deephaven CSV Issue #52</a>. When the
     * bug exists, the library hangs (and this test times out). When the bug is fixed, the test succeeds.
     */
    @Test
    @Timeout(value = 30)
    public void bug52() throws CsvReaderException {
        final int numRows = 50_000_000;
        final RepeatingInputStream inputStream =
                new RepeatingInputStream("Col1,Col2,Col3\n", "1.1,2.2,null\n", numRows);
        final CsvSpecs specs = defaultCsvBuilder()
                .parsers(Collections.singletonList(Parsers.DOUBLE))
                .nullValueLiterals(Collections.singletonList("null"))
                .build();
        final SinkFactory sf = makeBlackholeSinkFactoryWithSynchronizingDoubleSink(3, 1_000_000);
        CsvReader.read(specs, inputStream, sf);
    }

    /**
     * Reported in <a href="https://github.com/deephaven/deephaven-core/issues/2898">Deephaven Core Issue #2898</a>. Bug
     * filed in <a href="https://github.com/deephaven/deephaven-csv/issues/70">Deephaven CSV Issue #70</a>.
     */
    @Test
    public void bug70() throws CsvReaderException {
        final String input = "Coin,Change,Remark\r\n" +
                "USDT,-49.00787612,\r\n" +
                "USDT,-152.686844,穿仓保证金补偿\r\n" +
                "USDT,-59.92650232,\r\n" +
                "USDT,-102.3862566,\r\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Coin", "USDT", "USDT", "USDT", "USDT"),
                        Column.ofValues("Change", -49.00787612, -152.686844, -59.92650232, -102.3862566),
                        Column.ofRefs("Remark", null, "穿仓保证金补偿", null, null));
        invokeTest(defaultCsvBuilder().parsers(Parsers.DEFAULT).build(), input, expected);
    }

    /**
     * Addresses <a href="https://github.com/deephaven/deephaven-csv/issues/101">Deephaven CSV Issue #101</a>. When the
     * bug exists, the library deadlocks (and this test times out). When the bug is fixed, the test succeeds.
     */
    @Test
    public void bug101() throws CsvReaderException {
        final int numCharsInBigCell = DenseStorageConstants.LARGE_THRESHOLD - 1;
        final int numStringsThatFitInAQueueBlock = DenseStorageConstants.PACKED_QUEUE_SIZE / numCharsInBigCell;
        final int numRowsThatWillTriggerTheDeadlock =
                numStringsThatFitInAQueueBlock * (DenseStorageConstants.MAX_UNOBSERVED_BLOCKS + 1);

        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numCharsInBigCell; ++i) {
            sb.append('X');
        }
        sb.append('\n');
        final String bigCell = sb.toString();

        final RepeatingInputStream inputStream =
                new RepeatingInputStream("Col1\n", bigCell, numRowsThatWillTriggerTheDeadlock);
        final CsvSpecs specs = defaultCsvBuilder()
                .parsers(Collections.singletonList(Parsers.STRING))
                .build();
        CsvReader.read(specs, inputStream, makeMySinkFactory());
    }

    /**
     * Addresses <a href="https://github.com/deephaven/deephaven-csv/issues/133">Deephaven CSV Issue #133</a>. The
     * library was not trimming trailing whitespace in unquoted strings when requested.
     */
    @Test
    public void bug133() throws CsvReaderException {
        final String input = "String1,String2\n" +
                "hello   ,   there\n";

        final ColumnSet includingSpaces =
                ColumnSet.of(
                        Column.ofRefs("String1", "hello   "), // including surrounding spaces
                        Column.ofRefs("String2", "   there"));

        final ColumnSet ignoringSpaces =
                ColumnSet.of(
                        Column.ofRefs("String1", "hello"), // ignoring surrounding spaces
                        Column.ofRefs("String2", "there"));

        invokeTest(defaultCsvBuilder().parsers(Parsers.DEFAULT).ignoreSurroundingSpaces(false).build(), input,
                includingSpaces);
        invokeTest(defaultCsvBuilder().parsers(Parsers.DEFAULT).ignoreSurroundingSpaces(true).build(), input,
                ignoringSpaces);
    }

    /**
     * Reported in <a href="https://github.com/deephaven/deephaven-csv/issues/162">Deephaven CSV Issue #162</a>. The
     * library was throwing an internal error when the last cell is empty and the last line is not terminated with a
     * newline.
     */
    @Test
    public void bug162() throws CsvReaderException {
        // Last cell is empty and the line is not terminated.
        final String input = "A,B\n" +
                "apple,banana\n" +
                "cherry,";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("A", "apple", "cherry"),
                        Column.ofRefs("B", "banana", null));
        invokeTest(defaultCsvBuilder().parsers(Parsers.DEFAULT).build(), input, expected);
    }

    /**
     * Reported in <a href="https://github.com/deephaven/deephaven-csv/issues/190">Deephaven CSV Issue #190</a>. That
     * issue report misidentifies the root cause as having to do with reserved keywords. This is not correct because the
     * library doesn't care whether a column header is a reserved keyword. The actual root cause is an interaction
     * between the user-supplied "legalizer" and user-specified parsers or null literals that are specified by column
     * names. Specifically the question is whether column names mentioned in {@link CsvSpecs.Builder#putParserForName}
     * and {@link CsvSpecs.Builder#putNullValueLiteralsForName} should refer to the name that the column had *before* it
     * was transformed by the legalizer, or *after*. The expected behavior is "before", but prior to this fix the
     * library was doing the "after" behavior. This is a parameterized test that invokes the behavior for {delimited,
     * fixed columns} x {without and with a legalizer}.
     */
    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    public void bug190(boolean hasFixedWidthColumns, boolean invokeLegalizer) throws CsvReaderException {
        // +++ is the null value literal for Col1
        // *** is the null value literal for Col2
        // ??? is the null value literal for Col3

        final String input;

        if (!hasFixedWidthColumns) {
            input = "Col1,Col2,Col3\n" +
                    "+++,20,30\n" +
                    "100,***,300\n" +
                    "1000,2000,???\n";
        } else {
            input = "Col1 Col2 Col3\n" +
                    "+++  20   30\n" +
                    "100  ***  300\n" +
                    "1000 2000 ???\n";
        }

        final String[] expectedColumnNames = !invokeLegalizer ? new String[] {"Col1", "Col2", "Col3"}
                : new String[] {"xyzCol1", "xyzCol2", "xyzCol3"};

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(expectedColumnNames[0], Sentinels.NULL_LONG, (long) 100, (long) 1000),
                        Column.ofValues(expectedColumnNames[1], (double) 20, Sentinels.NULL_DOUBLE, (double) 2000),
                        Column.ofRefs(expectedColumnNames[2], "30", "300", null));

        Function<String[], String[]> legalizer = in -> {
            for (int i = 0; i != in.length; ++i) {
                // e.g. transform Col1 to xyzCol1
                in[i] = "xyz" + in[i];
            }
            return in;
        };

        CsvSpecs.Builder specsBase =
                defaultCsvBuilder().hasFixedWidthColumns(hasFixedWidthColumns).parsers(Parsers.DEFAULT)
                        .putParserForName("Col1", Parsers.LONG).putParserForName("Col2", Parsers.DOUBLE)
                        .putParserForName("Col3", Parsers.STRING)
                        .putNullValueLiteralsForName("Col1", Collections.singletonList("+++"))
                        .putNullValueLiteralsForName("Col2", Collections.singletonList("***"))
                        .putNullValueLiteralsForName("Col3", Collections.singletonList("???"));

        if (invokeLegalizer) {
            specsBase = specsBase.headerLegalizer(legalizer);
        }

        invokeTest(specsBase.build(), input, expected);
    }

    @Test
    public void validates() {
        final String lengthyMessage = "CsvSpecs failed validation for the following reasons: "
                + "quote is set to '€' but is required to be 7-bit ASCII, "
                + "delimiter is set to '€' but is required to be 7-bit ASCII, "
                + "skipRows is set to -2, but is required to be nonnegative, "
                + "skipHeaderRows is set to -5, but is required to be nonnegative, "
                + "numRows is set to -5, but is required to be nonnegative";
        Assertions
                .assertThatThrownBy(() -> CsvSpecs.builder().numRows(-5).skipHeaderRows(-5).skipRows(-2).delimiter('€')
                        .quote('€').build())
                .hasMessage(lengthyMessage);
    }

    @Test
    public void validatesHeaderRowConsistency() {
        final String lengthyMessage = "CsvSpecs failed validation for the following reasons: "
                + "skipHeaderRows != 0 but hasHeaderRow is not set";
        Assertions
                .assertThatThrownBy(() -> CsvSpecs.builder().hasHeaderRow(false).skipHeaderRows(2).build())
                .hasMessage(lengthyMessage);
    }

    @Test
    public void countsAreCorrect() throws CsvReaderException {
        final String input = "" + "Values\n" + "1\n" + "\n" + "3\n";
        final CsvReader.Result result = parse(defaultCsvSpecs(), toInputStream(input));
        Assertions.assertThat(result.numCols()).isEqualTo(1);
        Assertions.assertThat(result.numRows()).isEqualTo(3);
    }

    @Test
    public void countsAreCorrectNoTrailingNewline() throws CsvReaderException {
        final String input = "" + "Values\n" + "1\n" + "\n" + "3";
        final CsvReader.Result result = parse(defaultCsvSpecs(), toInputStream(input));
        Assertions.assertThat(result.numCols()).isEqualTo(1);
        Assertions.assertThat(result.numRows()).isEqualTo(3);
    }

    @Test
    public void countsAreCorrectHeaderless() throws CsvReaderException {
        final String input = "" + "1\n" + "\n" + "3\n";
        final CsvReader.Result result =
                parse(defaultCsvBuilder().hasHeaderRow(false).headers(Collections.singletonList("Value")).build(),
                        toInputStream(input));
        Assertions.assertThat(result.numCols()).isEqualTo(1);
        Assertions.assertThat(result.numRows()).isEqualTo(3);
    }

    @Test
    public void multilineColumnName() throws CsvReaderException {
        final String input =
                ""
                        + "|Some\nInts|,|Some\rStrings|,|Some\r\nBools|,|Some\r\n\nDoubles|\n"
                        + "-3,foo,false,1.0\n"
                        + "4,bar,true,2.0\n"
                        + "-5,baz,false,3.0\n";
        final CsvReader.Result result = parse(defaultCsvBuilder().quote('|').build(), toInputStream(input));
        final ColumnSet cs = toColumnSet(result, null);
        Assertions.assertThat(cs.columns[0].name).isEqualTo("Some\nInts");
        Assertions.assertThat(cs.columns[1].name).isEqualTo("Some\rStrings");
        Assertions.assertThat(cs.columns[2].name).isEqualTo("Some\r\nBools");
        Assertions.assertThat(cs.columns[3].name).isEqualTo("Some\r\n\nDoubles");
    }

    @Test
    public void multilineColumnNameReportsCorrectRowNumber() {
        // Too many columns is an error.
        final String input =
                ""
                        + "|Some\nInts|,|Some\rStrings|,|Some\r\nBools|,|Some\r\n\nDoubles|\n"
                        + "-3,foo,false,1.0\n"
                        + "4,bar,true,2.0,quz\n"
                        + "-5,baz,false,3.0\n";
        Assertions.assertThatThrownBy(
                () -> parse(defaultCsvBuilder().quote('|').build(), toInputStream(input)))
                .hasRootCauseMessage("Row 8 has too many columns (expected 4)");
    }

    private static final String BOOLEAN_INPUT =
            "" + "Values\n" + "true\n" + "\n" + "false\n" + "True\n" + "False\n" + "TrUe\n" + "FALSE\n";

    @Test
    public void booleans() throws CsvReaderException {
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(
                                "Values",
                                (byte) 1,
                                Sentinels.NULL_BOOLEAN_AS_BYTE,
                                (byte) 0,
                                (byte) 1,
                                (byte) 0,
                                (byte) 1,
                                (byte) 0)
                                .reinterpret(boolean.class));

        invokeTest(defaultCsvSpecs(), BOOLEAN_INPUT, expected);
    }

    private static final String CHAR_INPUT =
            "" + "Values\n" + "A\n" + "\n" + "B\n" + "C\n" + "1\n" + "2\n" + "3\n";

    @Test
    public void chars() throws CsvReaderException {
        final ColumnSet expected =
                ColumnSet.of(Column.ofValues("Values", 'A', Sentinels.NULL_CHAR, 'B', 'C', '1', '2', '3'));

        invokeTest(defaultCsvSpecs(), CHAR_INPUT, expected);
    }

    @Test
    public void forbiddenNullChars() throws CsvReaderException {
        final String input = "" + "Values\n" + "A\n" + Sentinels.NULL_CHAR + "\n";

        // NULL_CHAR can't be parsed as char; will be promoted to String.
        final ColumnSet expected = ColumnSet.of(Column.ofRefs("Values", "A", "" + Sentinels.NULL_CHAR));

        invokeTest(defaultCsvSpecs(), input, expected);
    }

    private static final String BYTE_INPUT = "" + "Values\n" + "-127\n" + "\n" + "127\n";

    @Test
    public void byteViaInference() throws CsvReaderException {
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(
                                "Values", (byte) (Byte.MIN_VALUE + 1), Sentinels.NULL_BYTE, Byte.MAX_VALUE));

        invokeTest(defaultCsvBuilder().parsers(Parsers.COMPLETE).build(), BYTE_INPUT, expected);
    }

    @Test
    public void forbiddenNullBytes() throws CsvReaderException {
        final String input = "" + "Values\n" + "-127\n" + Sentinels.NULL_BYTE + "\n" + "127\n";
        // NULL_BYTE can't be parsed as char; will be promoted to short (because we're using
        // the Parsers.COMPLETE set of parsers, and short is in Parsers.COMPLETE set).
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(
                                "Values", (short) (Byte.MIN_VALUE + 1), Sentinels.NULL_BYTE, Byte.MAX_VALUE));

        invokeTest(defaultCsvBuilder().parsers(Parsers.COMPLETE).build(), input, expected);
    }

    @Test
    public void byteIsInt() throws CsvReaderException {
        // By default, byte will be parsed as int, because neither Parsers.BYTE nor Parsers.SHORT is in
        // Parsers.DEFAULT
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues("Values", (Byte.MIN_VALUE + 1), Sentinels.NULL_INT, Byte.MAX_VALUE));

        invokeTest(defaultCsvSpecs(), BYTE_INPUT, expected);
    }

    private static final String SHORT_INPUT = "" + "Values\n" + "-32767\n" + "\n" + "32767\n";

    @Test
    public void shorts() throws CsvReaderException {
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(
                                "Values", (short) (Short.MIN_VALUE + 1), Sentinels.NULL_SHORT, Short.MAX_VALUE));

        invokeTest(defaultCsvBuilder().parsers(Parsers.COMPLETE).build(), SHORT_INPUT, expected);
    }

    @Test
    public void forbiddenNullShorts() throws CsvReaderException {
        final String input = "" + "Values\n" + "-32767\n" + Sentinels.NULL_SHORT + "\n" + "32767\n";

        // NULL_SHORT can't be parsed as short; will be promoted to int.
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(
                                "Values", (int) (Short.MIN_VALUE + 1), Sentinels.NULL_SHORT, Short.MAX_VALUE));

        invokeTest(defaultCsvBuilder().parsers(Parsers.COMPLETE).build(), input, expected);
    }

    @Test
    public void ints() throws CsvReaderException {
        final String input = "" + "Values\n" + "-2147483647\n" + "\n" + "2147483647\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(
                                "Values", Integer.MIN_VALUE + 1, Sentinels.NULL_INT, Integer.MAX_VALUE));

        invokeTest(defaultCsvSpecs(), input, expected);
    }

    @Test
    public void forbiddenNullInts() throws CsvReaderException {
        final String input = "" + "Values\n" + Sentinels.NULL_INT + "\n";

        // NULL_INT can't be parsed as int; will be promoted to long.
        final ColumnSet expected = ColumnSet.of(Column.ofValues("Values", (long) Sentinels.NULL_INT));

        invokeTest(defaultCsvSpecs(), input, expected);
    }

    private static final String LONG_INPUT =
            "" + "Values\n" + "-9223372036854775807\n" + "\n" + "9223372036854775807\n";

    @Test
    public void longs() throws CsvReaderException {
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues("Values", Long.MIN_VALUE + 1, Sentinels.NULL_LONG, Long.MAX_VALUE));

        invokeTest(defaultCsvSpecs(), LONG_INPUT, expected);
    }

    @Test
    public void forbiddenNullLongs() throws CsvReaderException {
        final String input = "" + "Values\n" + Sentinels.NULL_LONG + "\n";

        // NULL_LONG can't be parsed as long; will be promoted to double.
        final ColumnSet expected =
                ColumnSet.of(Column.ofValues("Values", (double) Sentinels.NULL_LONG));

        invokeTest(defaultCsvSpecs(), input, expected);
    }

    @Test
    public void longAsStringsViaInference() throws CsvReaderException {
        final ColumnSet expected =
                ColumnSet.of(Column.ofRefs("Values", "-9223372036854775807", null, "9223372036854775807"));

        invokeTest(defaultCsvBuilder().parsers(Collections.singletonList(Parsers.STRING)).build(), LONG_INPUT,
                expected);
    }

    @Test
    public void longAsStringsViaParser() throws CsvReaderException {
        final ColumnSet expected =
                ColumnSet.of(Column.ofRefs("Values", "-9223372036854775807", null, "9223372036854775807"));

        invokeTest(defaultCsvBuilder().putParserForName("Values", Parsers.STRING).build(), LONG_INPUT, expected);
    }

    private static final String FLOAT_INPUT =
            ""
                    + "Values\n"
                    + "Infinity\n"
                    + "\n"
                    + "-Infinity\n"
                    + "NaN\n"
                    + "3.4028234e+38\n"
                    + "1.17549435E-38\n"
                    + "1.4e-45\n";

    @Test
    public void floatIsDouble() throws CsvReaderException {
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(
                                "Values",
                                Float.POSITIVE_INFINITY,
                                Sentinels.NULL_DOUBLE,
                                Float.NEGATIVE_INFINITY,
                                Float.NaN,
                                3.4028234e+38d,
                                1.17549435E-38d,
                                1.4e-45d));

        invokeTest(defaultCsvSpecs(), FLOAT_INPUT, expected);
    }

    @Test
    public void floatViaInference() throws CsvReaderException {
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(
                                "Values",
                                Float.POSITIVE_INFINITY,
                                Sentinels.NULL_FLOAT,
                                Float.NEGATIVE_INFINITY,
                                Float.NaN,
                                Float.MAX_VALUE,
                                Float.MIN_NORMAL,
                                Float.MIN_VALUE));

        invokeTest(defaultCsvBuilder().parsers(Collections.singletonList(Parsers.FLOAT_FAST)).build(), FLOAT_INPUT,
                expected);
    }

    @Test
    public void forbiddenNullFloats() throws CsvReaderException {
        final String input = "" + "Values\n" + Sentinels.NULL_FLOAT + "\n";

        // I wanted to say simply (double)Sentinels.NULL_FLOAT, but that's a different number from
        // the below (alas).
        final double nullFloatAsParsedByDouble = Double.parseDouble("" + Sentinels.NULL_FLOAT);

        // NULL_FLOAT can't be parsed as float; will be promoted to double.
        final ColumnSet expected = ColumnSet.of(Column.ofValues("Values", nullFloatAsParsedByDouble));

        invokeTest(defaultCsvBuilder().parsers(Parsers.COMPLETE).build(), input, expected);
    }

    private static final String DOUBLE_INPUT = ""
            + "Values\n"
            + "Infinity\n"
            + "\n"
            + "-Infinity\n"
            + "NaN\n"
            + "1.7976931348623157e+308\n"
            + "2.2250738585072014E-308\n"
            + "12.34\n"
            + "-56.78\n"
            + "4.9e-324\n";

    @Test
    public void doubleRange() throws CsvReaderException {
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(
                                "Values",
                                Double.POSITIVE_INFINITY,
                                Sentinels.NULL_DOUBLE,
                                Double.NEGATIVE_INFINITY,
                                Double.NaN,
                                Double.MAX_VALUE,
                                Double.MIN_NORMAL,
                                12.34,
                                -56.78,
                                Double.MIN_VALUE));
        invokeTest(defaultCsvSpecs(), DOUBLE_INPUT, expected);
    }

    @Test
    public void doubleRangeWithCustomPlugin() throws CsvReaderException {
        // This test makes sure that the parse double callback is actually being invoked.
        final String input = ""
                + "Values\n"
                + "1.1\n"
                + "2.2\n"
                + "3.3\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(
                                "Values",
                                2.1,
                                3.2,
                                4.3));

        // My "strange" parser adds 1.0 to the parsed value.
        Tokenizer.CustomDoubleParser myStrangeParser = new Tokenizer.CustomDoubleParser() {
            @Override
            public double parse(ByteSlice bs) throws NumberFormatException {
                return parse((CharSequence) bs);
            }

            @Override
            public double parse(CharSequence cs) throws NumberFormatException {
                return Double.parseDouble(cs.toString()) + 1;
            }
        };

        invokeTest(defaultCsvBuilder()
                .customDoubleParser(myStrangeParser).build(), input, expected);
    }

    @Test
    public void forbiddenNullDoubles() throws CsvReaderException {
        final String input = "" + "Values\n" + Sentinels.NULL_DOUBLE + "\n";

        // NULL_DOUBLE can't be parsed as double; will be promoted to String
        final ColumnSet expected = ColumnSet.of(Column.ofRefs("Values", Sentinels.NULL_DOUBLE + ""));

        invokeTest(defaultCsvBuilder().parsers(Parsers.COMPLETE).build(), input, expected);
    }

    private static final String SKIPPED_INPUT = ""
            + "Values\n"
            + "1\n"
            + "2\n"
            + "3\n"
            + "4\n"
            + "5\n"
            + "6\n"
            + "7\n"
            + "8\n"
            + "9\n";

    @Test
    public void skippedAndLimitedInts() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(Column.ofValues("Values", 4, 5, 6, 7));

        invokeTest(defaultCsvBuilder().skipRows(3).numRows(4).build(), SKIPPED_INPUT, expected);
    }

    @Test
    public void skippedInts() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(Column.ofValues("Values", 4, 5, 6, 7, 8, 9));

        invokeTest(defaultCsvBuilder().skipRows(3).build(), SKIPPED_INPUT, expected);
    }

    private static final String SKIPPED_HEADER_ROW_INPUT = ""
            + "Abitrary,input,data\n"
            + "\n"
            + "X,Y,Z,1,2,3\n"
            + "Values\n"
            + "1\n"
            + "2\n"
            + "3\n"
            + "4\n"
            + "5\n"
            + "6\n"
            + "7\n"
            + "8\n"
            + "9\n";

    @Test
    public void skippedHeaderRows() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(Column.ofValues("Values", 7, 8, 9));

        invokeTest(defaultCsvBuilder().skipHeaderRows(3).skipRows(6).build(), SKIPPED_HEADER_ROW_INPUT, expected);
    }

    private static final String MULTIPLE_NULLS_INPUT = ""
            + "Col1,Col2\n"
            + "1,aaaaa\n"
            + "2,bbbbb\n"
            + ",ccccc\n" // cell for Col1 is blank
            + "*NULL*,ddddd\n"
            + "3,*NULL*\n";

    @Test
    public void multipleNullLiterals() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Col1", 1, 2, Sentinels.NULL_INT, Sentinels.NULL_INT, 3),
                Column.ofRefs("Col2", "aaaaa", "bbbbb", "ccccc", "ddddd", null));

        final CsvSpecs specs = defaultCsvBuilder().nullValueLiterals(Arrays.asList("", "*NULL*")).build();
        invokeTest(specs, MULTIPLE_NULLS_INPUT, expected);
    }

    @Test
    public void perColumnNullLiterals() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Col1", 1, 2, Sentinels.NULL_INT, Sentinels.NULL_INT, 3),
                Column.ofRefs("Col2", "aaaaa", null, "ccccc", null, "*NULL*"));

        final CsvSpecs specs = defaultCsvBuilder()
                .nullValueLiterals(Arrays.asList("", "*NULL*"))
                .putNullValueLiteralsForName("Col2", Arrays.asList("bbbbb", "ddddd"))
                .build();
        invokeTest(specs, MULTIPLE_NULLS_INPUT, expected);
    }


    private static final String EMPTY_LINES_INPUT = ""
            + "Col1,Col2\n"
            + "aa,bb\n"
            + "cc,dd\n"
            + "\n"
            + "ee,ff\n"
            + "\n"
            + "gg,hh\n";

    @Test
    public void emptyLinesNull() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("Col1", "aa", "cc", null, "ee", null, "gg"),
                Column.ofRefs("Col2", "bb", "dd", null, "ff", null, "hh"));

        invokeTest(defaultCsvBuilder().allowMissingColumns(true).ignoreEmptyLines(false).build(), EMPTY_LINES_INPUT,
                expected);
    }

    @Test
    public void emptyLinesIgnored() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("Col1", "aa", "cc", "ee", "gg"),
                Column.ofRefs("Col2", "bb", "dd", "ff", "hh"));

        invokeTest(defaultCsvBuilder().allowMissingColumns(false).ignoreEmptyLines(true).build(), EMPTY_LINES_INPUT,
                expected);
    }

    @Test
    public void emptyLinesError() {
        Assertions
                .assertThatThrownBy(
                        () -> invokeTest(defaultCsvBuilder().allowMissingColumns(false).ignoreEmptyLines(false).build(),
                                EMPTY_LINES_INPUT,
                                ColumnSet.NONE))
                .hasRootCauseMessage("Row 4 has too few columns (expected 2)");
    }

    private static final String VARIETY_OF_NUMERICS_INPUT = ""
            + "Values\n"
            + "\n" // NULL
            + "\n" // NULL
            + "0\n" // byte
            + "1\n" // byte
            + "300\n" // short
            + "400\n" // short
            + "100000\n" // int
            + "100001\n" // int
            + "3000000000\n" // long
            + "1234.5678\n"; // double


    @Test
    public void varietyOfNumerics() throws CsvReaderException {
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(
                                "Values",
                                Sentinels.NULL_DOUBLE,
                                Sentinels.NULL_DOUBLE,
                                0.0,
                                1.0,
                                300.0,
                                400.0,
                                100000.0,
                                100001.0,
                                3000000000.0,
                                1234.5678));
        invokeTest(defaultCsvBuilder().parsers(Parsers.COMPLETE).build(), VARIETY_OF_NUMERICS_INPUT, expected);
    }

    @Test
    public void strings() throws CsvReaderException {
        final String input =
                ""
                        + "Values\n"
                        + "\"Hello, world\"\n"
                        + "\n"
                        + // the empty string is null
                        "Goodbye.\n";

        final ColumnSet expected =
                ColumnSet.of(Column.ofRefs("Values", "Hello, world", null, "Goodbye."));

        invokeTest(defaultCsvSpecs(), input, expected);
    }

    @Test
    public void multi() throws CsvReaderException {
        // These are columns of data. We are going to mix and match them.
        final String booleanInput = "false\ntrUe\nFaLsE\n";
        final String byteInput1 = "1\n2\n3\n";
        final String byteInput2 = "-1\n-2\n-3\n";
        final String shortInput = "300\n301\n302\n";
        final String intInput = "50000\n50001\n50002\n";
        final String longInput = "3000000000\n3000000001\n3000000002\n";
        final String doubleInput = "123.456\n234.567e25\n987.654e-20\n";
        final String dateTimeInput =
                "1966-03-01 12:34:56Z\n1977-02-08 03:04:05Z\n1989-11-11 11:11:11Z\n";
        final String charInput = "a\nb\nc\n";
        final String stringInput = "Deephaven\nStreaming\nJoins\n";

        final String[] allInputs = {
                booleanInput,
                byteInput1,
                byteInput2,
                shortInput,
                intInput,
                longInput,
                doubleInput,
                dateTimeInput,
                charInput,
                stringInput
        };
        final Class<?>[] expectedTypes = {
                boolean.class,
                byte.class,
                byte.class,
                short.class,
                int.class,
                long.class,
                double.class,
                Instant.class,
                char.class,
                String.class
        };
        final boolean[] entriesAreAllNullOrOneChar = {
                false, true, false, false, false, false, false, false, true, false
        };

        for (int ii = 0; ii < allInputs.length; ++ii) {
            for (int jj = 0; jj < allInputs.length; ++jj) {
                final boolean oneCharIJ = entriesAreAllNullOrOneChar[ii] && entriesAreAllNullOrOneChar[jj];
                final Class<?> inferredIJ =
                        SimpleInferrer.infer(expectedTypes[ii], expectedTypes[jj], oneCharIJ);
                for (int kk = 0; kk < allInputs.length; ++kk) {
                    final boolean oneCharIJK = oneCharIJ && entriesAreAllNullOrOneChar[kk];
                    final Class<?> expectedType =
                            SimpleInferrer.infer(expectedTypes[kk], inferredIJ, oneCharIJK);
                    final String input = "Values\n" + allInputs[ii] + allInputs[jj] + allInputs[kk];
                    final InputStream inputStream = toInputStream(input);
                    final CsvSpecs specs = defaultCsvBuilder().parsers(Parsers.COMPLETE).build();
                    final ColumnSet columnSet = toColumnSet(parse(specs, inputStream), null);
                    final Class<?> actualType = columnSet.columns[0].reinterpretedType;
                    Assertions.assertThat(actualType)
                            .withFailMessage(
                                    "Expected to infer type %s; actually inferred %s. Failing input: %s",
                                    expectedType.getCanonicalName(), actualType.getCanonicalName(), input)
                            .isEqualTo(expectedType);
                }
            }
        }
    }

    private static class SimpleInferrer {
        private static final int BOOLEAN = 1;
        private static final int DATETIME = 2;
        private static final int STRING = 3;
        private static final int CHAR = 4;
        private static final int BYTE = 5;
        private static final int SHORT = 6;
        private static final int INT = 7;
        private static final int LONG = 8;
        private static final int FLOAT = 9;
        private static final int DOUBLE = 10;

        public static Class<?> infer(
                final Class<?> type1, final Class<?> type2, final boolean allNullOrOneChar) {
            // Same types yield that type.
            if (type1 == type2) {
                return type1;
            }

            final int priority1 = getPriority(type1);
            final int priority2 = getPriority(type2);

            final int highestPriority = Math.min(priority1, priority2);
            final Class<?> widestType = priority1 < priority2 ? type2 : type1;

            // (Boolean, DateTime, or String) and (something else) yields String.
            if (highestPriority == BOOLEAN || highestPriority == DATETIME || highestPriority == STRING) {
                return String.class;
            }

            // Char paired with some numeric will yield char if the numerics are one digit wide; otherwise
            // String
            if (highestPriority == CHAR) {
                return allNullOrOneChar ? char.class : String.class;
            }

            // Numeric types yield the widest type.
            return widestType;
        }

        private static int getPriority(Class<?> type) {
            if (type == boolean.class)
                return BOOLEAN;
            if (type == Instant.class)
                return DATETIME;
            if (type == char.class)
                return CHAR;
            if (type == String.class)
                return STRING;
            if (type == byte.class)
                return BYTE;
            if (type == short.class)
                return SHORT;
            if (type == int.class)
                return INT;
            if (type == long.class)
                return LONG;
            if (type == float.class)
                return FLOAT;
            if (type == double.class)
                return DOUBLE;
            throw new RuntimeException("Unexpected type " + type.getCanonicalName());
        }
    }

    @Test
    public void quotingSuccessfulEdgeCases() throws CsvReaderException {
        final String input =
                ""
                        + "Values\n"
                        + "##\n"
                        + // the empty string, which is configured below to give us NULL
                        "####\n"
                        + // #
                        "######\n"; // ##

        final ColumnSet expected = ColumnSet.of(Column.ofRefs("Values", null, "#", "##"));

        invokeTest(defaultCsvBuilder().quote('#').build(), input, expected);
    }

    @Test
    public void quotingFailingEdgeCases() {
        final String input = "" + "Values\n" + "###\n"; // invalid

        Assertions.assertThatThrownBy(
                () -> invokeTest(defaultCsvBuilder().quote('#').build(), input, ColumnSet.NONE))
                .hasRootCauseMessage("Cell did not have closing quote character");
    }

    @Test
    public void quotingExcessMaterial() {
        final String input = "" + "Val1,Val2\n" + "#hello#junk,there\n"; // invalid

        Assertions.assertThatThrownBy(
                () -> invokeTest(defaultCsvBuilder().quote('#').build(), input, ColumnSet.NONE))
                .hasRootCauseMessage("Logic error: final non-whitespace in field is not quoteChar");
    }

    @Test
    public void stringWithNullLiteralSetAndValueNull() throws CsvReaderException {
        // It should work when the null literal is set to something special, but the null String value
        // is the null
        // reference.
        final String input = "" + "Values\n" + "hello\n" + "NULL\n";

        final ColumnSet expected = ColumnSet.of(Column.ofRefs("Values", "hello", null));

        invokeTest(defaultCsvBuilder().nullValueLiterals(Collections.singletonList("NULL")).build(), input, expected);
    }

    @Test
    public void stringsPound() throws CsvReaderException {
        final String input = "" + "Values\n" + "#Hello, world#\n" + "\n" + "Goodbye.\n";

        final ColumnSet expected =
                ColumnSet.of(Column.ofRefs("Values", "Hello, world", null, "Goodbye."));

        invokeTest(defaultCsvBuilder().quote('#').build(), input, expected);
    }

    @Test
    public void newlineDiversity() throws CsvReaderException {
        final String input = "" + "Values\r" + "-2147483647\r\n" + "\n" + "2147483647\r\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(
                                "Values", Integer.MIN_VALUE + 1, Sentinels.NULL_INT, Integer.MAX_VALUE));

        invokeTest(defaultCsvSpecs(), input, expected);
    }

    @Test
    public void overrideHeaders() throws CsvReaderException {
        final String input = "" + "Foo,Bar,Baz\n" + "1,2,3\n" + "4,5,6\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues("A", 1, 4), Column.ofValues("Qux", 2, 5), Column.ofValues("C", 3, 6));

        invokeTest(defaultCsvBuilder().headers(Arrays.asList("A", "B", "C")).putHeaderForIndex(1, "Qux").build(), input,
                expected);
    }

    private static final String LANGUAGE_EXAMPLE_HEADERLESS_INPUT =
            ""
                    + "C,Dennis Ritchie,Compiled\n"
                    + "C++,Bjarne Stroustrup,Compiled\n"
                    + "Fortran,John Backus,Compiled\n"
                    + "Java,James Gosling,Both\n"
                    + "JavaScript,Brendan Eich,Interpreted\n"
                    + "MATLAB,Cleve Moler,Interpreted\n"
                    + "Pascal,Niklaus Wirth,Compiled\n"
                    + "Python,Guido van Rossum,Interpreted\n";

    private static final String LANGUAGE_EXAMPLE_INPUT =
            "" + "Language,Creator,Type\n" + LANGUAGE_EXAMPLE_HEADERLESS_INPUT;

    private static final String LANGUAGE_EXAMPLE_TSV =
            ""
                    + "Language\tCreator\tType\n"
                    + "C\tDennis Ritchie\tCompiled\n"
                    + "C++\tBjarne Stroustrup\tCompiled\n"
                    + "Fortran\tJohn Backus\tCompiled\n"
                    + "Java\tJames Gosling\tBoth\n"
                    + "JavaScript\tBrendan Eich\tInterpreted\n"
                    + "MATLAB\tCleve Moler\tInterpreted\n"
                    + "Pascal\tNiklaus Wirth\tCompiled\n"
                    + "Python\tGuido van Rossum\tInterpreted\n";

    @Test
    public void languageExample() throws CsvReaderException {
        invokeTest(defaultCsvSpecs(), LANGUAGE_EXAMPLE_INPUT, languageCreatorTypeTable());
    }

    @Test
    public void languageExampleTsv() throws CsvReaderException {
        invokeTest(
                defaultCsvBuilder().delimiter('\t').build(),
                LANGUAGE_EXAMPLE_TSV,
                languageCreatorTypeTable());
    }

    @Test
    public void languageExampleHeaderless() throws CsvReaderException {
        invokeTest(
                defaultCsvBuilder().hasHeaderRow(false).build(),
                LANGUAGE_EXAMPLE_HEADERLESS_INPUT,
                languageCreatorTypeTableHeaderless());
    }

    @Test
    public void languageExampleHeaderlessExplicit() throws CsvReaderException {
        final ColumnSet expected = languageCreatorTypeTable();
        invokeTest(
                defaultCsvBuilder().hasHeaderRow(false).headers(Arrays.asList("Language", "Creator", "Type")).build(),
                LANGUAGE_EXAMPLE_HEADERLESS_INPUT,
                expected);
    }

    private static ColumnSet languageCreatorTypeTable() {
        return populateLanguageExample("Language", "Creator", "Type");
    }

    private static ColumnSet languageCreatorTypeTableHeaderless() {
        return populateLanguageExample("Column1", "Column2", "Column3");
    }

    private static ColumnSet populateLanguageExample(
            final String col1, final String col2, final String col3) {
        return ColumnSet.of(
                Column.ofRefs(
                        col1, "C", "C++", "Fortran", "Java", "JavaScript", "MATLAB", "Pascal", "Python"),
                Column.ofRefs(
                        col2,
                        "Dennis Ritchie",
                        "Bjarne Stroustrup",
                        "John Backus",
                        "James Gosling",
                        "Brendan Eich",
                        "Cleve Moler",
                        "Niklaus Wirth",
                        "Guido van Rossum"),
                Column.ofRefs(
                        col3,
                        "Compiled",
                        "Compiled",
                        "Compiled",
                        "Both",
                        "Interpreted",
                        "Interpreted",
                        "Compiled",
                        "Interpreted"));
    }

    private static final String WHITESPACE_NO_QUOTES =
            ""
                    + "Sym,Type,Price,SecurityId\n"
                    + "GOOG, Dividend, 0.25, 200\n"
                    + "T, Dividend, 0.15, 300\n"
                    + " Z, Dividend, 0.18, 500\n";

    @Test
    public void whitespaceNoQuotes() throws CsvReaderException {
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Sym", "GOOG", "T", "Z"),
                        Column.ofRefs("Type", "Dividend", "Dividend", "Dividend"),
                        Column.ofValues("Price", 0.25, 0.15, 0.18),
                        Column.ofValues("SecurityId", 200, 300, 500));

        invokeTest(defaultCsvSpecs(), WHITESPACE_NO_QUOTES, expected);
    }

    @Test
    public void whitespaceNoQuotesLiteral() throws CsvReaderException {
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Sym", "GOOG", "T", " Z"),
                        Column.ofRefs("Type", " Dividend", " Dividend", " Dividend"),
                        Column.ofValues("Price", 0.25, 0.15, 0.18),
                        Column.ofValues("SecurityId", 200, 300, 500));

        invokeTest(
                defaultCsvBuilder().ignoreSurroundingSpaces(false).build(), WHITESPACE_NO_QUOTES, expected);
    }

    @Test
    public void whitespaceOutside() throws CsvReaderException {
        // Use vertical bars instead of quotation marks to make things more readable for the humans
        // looking at this.
        final String input =
                (""
                        + "Sym,Type,Price,SecurityId\n"
                        + "|GOOG|, |Dividend|, |0.25|, |200|\n"
                        + "|T|, |Dividend|, |0.15|, |300|\n"
                        + " |Z|, |Dividend|, |0.18|, |500|\n");

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Sym", "GOOG", "T", "Z"),
                        Column.ofRefs("Type", "Dividend", "Dividend", "Dividend"),
                        Column.ofValues("Price", 0.25, 0.15, 0.18),
                        Column.ofValues("SecurityId", 200, 300, 500));

        invokeTest(defaultCsvBuilder().quote('|').build(), input, expected);
    }

    // Use vertical bars instead of quotation marks to make things more readable for the humans
    // looking at this.
    private static final String WHITESPACE_INSIDE =
            ""
                    + "Sym,Type,Price,SecurityId\n"
                    + "|GOOG|,| Dividend|,| 0.25|,| 200|\n"
                    + "|T|,|Dividend |,| 0.15|,| 300|\n"
                    + "| Z|,| Dividend |,| 0.18|,| 500|\n";

    @Test
    public void whitespaceInsideDefault() throws CsvReaderException {
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Sym", "GOOG", "T", " Z"),
                        Column.ofRefs("Type", " Dividend", "Dividend ", " Dividend "),
                        Column.ofValues("Price", 0.25, 0.15, 0.18),
                        Column.ofValues("SecurityId", 200, 300, 500));
        invokeTest(defaultCsvBuilder().quote('|').build(), WHITESPACE_INSIDE, expected);
    }

    @Test
    public void whitespaceInsideTrim() throws CsvReaderException {
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Sym", "GOOG", "T", "Z"),
                        Column.ofRefs("Type", "Dividend", "Dividend", "Dividend"),
                        Column.ofValues("Price", 0.25, 0.15, 0.18),
                        Column.ofValues("SecurityId", 200, 300, 500));

        invokeTest(defaultCsvBuilder().quote('|').trim(true).build(), WHITESPACE_INSIDE, expected);
    }

    private static final String WHITESPACE_INSIDE_AND_OUTSIDE =
            ""
                    + "Sym,Type,Price,SecurityId\n"
                    + "|GOOG|, | Dividend|, | 0.25|, | 200|\n"
                    + "|T|, | Dividend|, | 0.15|, | 300|\n"
                    + "| Z|, | Dividend|, | 0.18|, | 500|\n";

    @Test
    public void whitespaceInsideAndOutsideDefault() throws CsvReaderException {
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Sym", "GOOG", "T", " Z"),
                        Column.ofRefs("Type", " Dividend", " Dividend", " Dividend"),
                        Column.ofValues("Price", 0.25, 0.15, 0.18),
                        Column.ofValues("SecurityId", 200, 300, 500));

        invokeTest(defaultCsvBuilder().quote('|').build(), WHITESPACE_INSIDE_AND_OUTSIDE, expected);
    }

    @Test
    public void whitespaceInsideAndOutsideTrim() throws CsvReaderException {
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Sym", "GOOG", "T", "Z"),
                        Column.ofRefs("Type", "Dividend", "Dividend", "Dividend"),
                        Column.ofValues("Price", 0.25, 0.15, 0.18),
                        Column.ofValues("SecurityId", 200, 300, 500));

        invokeTest(
                defaultCsvBuilder().quote('|').trim(true).build(),
                WHITESPACE_INSIDE_AND_OUTSIDE,
                expected);
    }

    @Test
    public void noTrailingNewlineHeaderOnly() throws CsvReaderException {
        // Sometimes there is no trailing newline. That's OK.
        final String input = "" + "Values1,Values2";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofArray("Values1", new short[0], 0), Column.ofArray("Values2", new short[0], 0));

        invokeTest(defaultCsvBuilder().nullParser(Parsers.SHORT).build(), input, expected);
    }

    @Test
    public void noTrailingNewline() throws CsvReaderException {
        // Sometimes there is no trailing newline. That's OK.
        final String input = "" + "SomeInts,SomeStrings\n" + "-3,foo\n" + "4,bar\n" + "-5,baz";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues("SomeInts", -3, 4, -5),
                        Column.ofRefs("SomeStrings", "foo", "bar", "baz"));

        invokeTest(defaultCsvSpecs(), input, expected);
    }

    @Test
    public void tooFewColumnsWithFinalNewline() throws CsvReaderException {
        // If there are too few columns, we just pad with null literals.
        final String input = "" + "A,B,C,D\n" + "-3,foo,1.2,false\n" + "4,bar,3.4,true\n" + "-5\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues("A", -3, 4, -5),
                        Column.ofRefs("B", "foo", "bar", null),
                        Column.ofValues("C", 1.2, 3.4, Sentinels.NULL_DOUBLE),
                        Column.ofValues("D", (byte) 0, (byte) 1, Sentinels.NULL_BOOLEAN_AS_BYTE)
                                .reinterpret(boolean.class));

        invokeTest(defaultCsvSpecs(), input, expected);
    }

    @Test
    public void tooFewColumnsWithoutFinalNewline() throws CsvReaderException {
        // If there are too few columns, we just pad with null literals.
        final String input = "" + "A,B,C,D\n" + "-3,foo,1.2,false\n" + "4,bar,3.4,true\n" + "-5";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues("A", -3, 4, -5),
                        Column.ofRefs("B", "foo", "bar", null),
                        Column.ofValues("C", 1.2, 3.4, Sentinels.NULL_DOUBLE),
                        Column.ofValues("D", (byte) 0, (byte) 1, Sentinels.NULL_BOOLEAN_AS_BYTE)
                                .reinterpret(boolean.class));

        invokeTest(defaultCsvSpecs(), input, expected);
    }

    @Test
    public void tooFewColumnsNoNullLiteralIsError() {
        final String input = "" + "A,B,C,D\n" + "-3,foo,1.2,false\n" + "4,bar,3.4,true\n" + "-5\n";

        Assertions
                .assertThatThrownBy(
                        () -> invokeTest(
                                defaultCsvBuilder().allowMissingColumns(true).nullValueLiterals(Collections.emptyList())
                                        .build(),
                                input, ColumnSet.NONE))
                .hasRootCauseMessage(
                        "Row 4 is short, but can't null-fill it because there is no configured null value literal for column \"B\".");
    }

    @Test
    public void tooFewColumnsDisallowedIsError() {
        final String input = "" + "A,B,C,D\n" + "-3,foo,1.2,false\n" + "4,bar,3.4,true\n" + "-5\n";

        Assertions
                .assertThatThrownBy(
                        () -> invokeTest(defaultCsvBuilder().allowMissingColumns(false).build(), input, ColumnSet.NONE))
                .hasRootCauseMessage("Row 4 has too few columns (expected 4)");
    }

    @Test
    public void excessColumnsError() {
        final String input = "" + "SomeInts,SomeStrings\n" + "-3,foo\n" + "4,bar,quz\n" + "-5,baz\n";

        Assertions.assertThatThrownBy(() -> invokeTest(defaultCsvSpecs(), input, ColumnSet.NONE))
                .hasRootCauseMessage("Row 3 has too many columns (expected 2)");
    }

    @Test
    public void excessColumnsDropped() throws CsvReaderException {
        final String input = "" + "SomeInts,SomeStrings\n" + "-3,foo\n" + "4,bar,quz,zax\n" + "-5,baz\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues("SomeInts", -3, 4, -5),
                        Column.ofRefs("SomeStrings", "foo", "bar", "baz"));

        invokeTest(defaultCsvBuilder().ignoreExcessColumns(true).build(), input, expected);
    }

    private static final String SINGLE_COLUMN_EMPTY_ROW = ""
            + "SomeInts\n"
            + "3\n"
            + "\n"
            + "4\n"
            + "5\n";

    @Test
    public void singleColumnEmptyRowIsNull() throws CsvReaderException {
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues("SomeInts", 3, Sentinels.NULL_INT, 4, 5));

        invokeTest(defaultCsvBuilder().ignoreEmptyLines(false).build(), SINGLE_COLUMN_EMPTY_ROW, expected);
    }

    @Test
    public void singleColumnEmptyRowIsSkipped() throws CsvReaderException {
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues("SomeInts", 3, 4, 5));

        invokeTest(defaultCsvBuilder().ignoreEmptyLines(true).build(), SINGLE_COLUMN_EMPTY_ROW, expected);
    }

    @Test
    public void singleColumnEmptyRowIsString() throws CsvReaderException {
        // Thanks to type inference, if you can't skip the empty row and you can't parse it as null,
        // then it's the empty string!
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("SomeInts", "3", "", "4", "5"));

        invokeTest(defaultCsvBuilder().ignoreEmptyLines(false).nullValueLiterals(Collections.emptyList()).build(),
                SINGLE_COLUMN_EMPTY_ROW,
                expected);
    }

    @Test
    public void singleColumnEmptyRowIsError() {
        // If you can't skip the empty row, you can't parse it as null, and you can't parse it as String,
        // then you are really out of luck!
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("SomeInts", "3", "", "4", "5"));

        Assertions
                .assertThatThrownBy(
                        () -> invokeTest(
                                defaultCsvBuilder().ignoreEmptyLines(false).nullValueLiterals(Collections.emptyList())
                                        .parsers(Collections.singletonList(Parsers.INT)).build(),
                                SINGLE_COLUMN_EMPTY_ROW, expected))
                .hasRootCauseMessage(
                        "Parsing failed on input, with nothing left to fall back to. Parser io.deephaven.csv.parsers.IntParser successfully parsed 1 items before failure.");
    }

    @Test
    public void haveSomeParsersButNotTheOnesYouNeed() {
        final String input = ""
                + "SomeInts\n"
                + "3\n"
                + "4\n"
                + "5\n"
                + "6.6\n";

        // If you have some parsers but not the ones you need, you are out of luck.
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("SomeInts", "3", "4", "5", "6.6"));

        Assertions
                .assertThatThrownBy(
                        () -> invokeTest(
                                defaultCsvBuilder().ignoreEmptyLines(false).nullValueLiterals(Collections.emptyList())
                                        .parsers(Arrays.asList(Parsers.INT, Parsers.LONG)).build(),
                                input, expected))
                .hasRootCauseMessage(
                        "Consumed 3 numeric items, then encountered a non-numeric item but there are no char/string parsers available.");
    }

    @Test
    public void duplicateColumnName() {
        final String input = "" + "abc,xyz,abc\n" + "Hello,there,Deephaven\n";
        Assertions.assertThatThrownBy(() -> invokeTest(defaultCsvSpecs(), input, ColumnSet.NONE))
                .hasMessageContaining("Repeated headers: abc");
    }

    @Test
    public void trailingNullColumnElided() throws CsvReaderException {
        // A completely-empty rightmost column (corresponding to a text file with trailing field
        // delimiters on every
        // line) will just be dropped.
        final String input = "" + "abc,def,ghi,\n" + "Hello,there,Deephaven,\n" + "foo,bar,baz,\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("abc", "Hello", "foo"),
                        Column.ofRefs("def", "there", "bar"),
                        Column.ofRefs("ghi", "Deephaven", "baz"));

        invokeTest(defaultCsvSpecs(), input, expected);
    }

    @Test
    public void trailingNullColumnMustBeEmpty() {
        // A completely-empty rightmost column (corresponding to a text file with trailing field
        // delimiters on every
        // line) will just be dropped.
        final String input =
                "" + "abc,def,ghi,\n" + "Hello,there,Deephaven,\n" + "foo,bar,baz,nonempty\n";
        Assertions.assertThatThrownBy(() -> invokeTest(defaultCsvSpecs(), input, ColumnSet.NONE))
                .hasRootCauseMessage("Column assumed empty but contains data");
    }

    @Test
    public void dateTimes() throws CsvReaderException {
        final String input =
                "" + "Values\n" + "2021-09-27T19:00:00Z\n" + "\n" + "2021-09-27T20:00:00Z\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(
                                "Values", 1632769200000000000L, Sentinels.NULL_LONG, 1632772800000000000L)
                                .reinterpret(Instant.class));

        invokeTest(defaultCsvSpecs(), input, expected);
    }

    @Test
    public void dateTimeFormats() throws CsvReaderException {
        final String input =
                ""
                        + "Values\n"
                        + "20210927T19Z\n"
                        + "20210927 19Z\n"
                        + "20210927T1934Z\n"
                        + "20210927T193458Z\n"
                        + "20210927T193458.123Z\n"
                        + "20210927T193458.123456Z\n"
                        + "20210927T193458.123456789Z\n"
                        + "20210927T193458.123456789+0200\n"
                        + "20210927T193458.123456789-0330\n"
                        + "2021-09-27T19Z\n"
                        + "2021-09-27 19Z\n"
                        + "2021-09-27T19:34Z\n"
                        + "2021-09-27T19:34:58Z\n"
                        + "2021-09-27T19:34:58.123Z\n"
                        + "2021-09-27T19:34:58.123456Z\n"
                        + "2021-09-27T19:34:58.123456789Z\n"
                        + "2021-09-27T19:34:58.123456789+0200\n"
                        + "2021-09-27T19:34:58.123456789-0330\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(
                                "Values",
                                1632769200000000000L,
                                1632769200000000000L,
                                1632771240000000000L,
                                1632771298000000000L,
                                1632771298123000000L,
                                1632771298123456000L,
                                1632771298123456789L,
                                1632764098123456789L,
                                1632783898123456789L,
                                1632769200000000000L,
                                1632769200000000000L,
                                1632771240000000000L,
                                1632771298000000000L,
                                1632771298123000000L,
                                1632771298123456000L,
                                1632771298123456789L,
                                1632764098123456789L,
                                1632783898123456789L)
                                .reinterpret(Instant.class));

        invokeTest(defaultCsvSpecs(), input, expected);
    }

    @Test
    public void timestampSeconds() throws CsvReaderException {
        final String input = "" + "Values\n" + "1632769200\n" + "\n" + "1632772800\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(
                                "Values",
                                1632769200000000000L,
                                Sentinels.NULL_TIMESTAMP_AS_LONG,
                                1632772800000000000L)
                                .reinterpret(Instant.class));

        invokeTest(defaultCsvBuilder().parsers(Collections.singletonList(Parsers.TIMESTAMP_SECONDS)).build(), input,
                expected);
    }

    @Test
    public void timestampMillis() throws CsvReaderException {
        final String input = "" + "Values\n" + "1632769200000\n" + "\n" + "1632772800000\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(
                                "Values",
                                1632769200000000000L,
                                Sentinels.NULL_TIMESTAMP_AS_LONG,
                                1632772800000000000L)
                                .reinterpret(Instant.class));

        invokeTest(defaultCsvBuilder().parsers(Collections.singletonList(Parsers.TIMESTAMP_MILLIS)).build(), input,
                expected);
    }

    @Test
    public void timestampMicros() throws CsvReaderException {
        final String input = "" + "Values\n" + "1632769200000000\n" + "\n" + "1632772800000000\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(
                                "Values",
                                1632769200000000000L,
                                Sentinels.NULL_TIMESTAMP_AS_LONG,
                                1632772800000000000L)
                                .reinterpret(Instant.class));

        invokeTest(defaultCsvBuilder().parsers(Collections.singletonList(Parsers.TIMESTAMP_MICROS)).build(), input,
                expected);
    }

    @Test
    public void timestampNanos() throws CsvReaderException {
        final String input = "" + "Values\n" + "1632769200000000000\n" + "\n" + "1632772800000000000\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(
                                "Values",
                                1632769200000000000L,
                                Sentinels.NULL_TIMESTAMP_AS_LONG,
                                1632772800000000000L)
                                .reinterpret(Instant.class));

        invokeTest(defaultCsvBuilder().parsers(Collections.singletonList(Parsers.TIMESTAMP_NANOS)).build(), input,
                expected);
    }

    @Test
    public void dateTimeCustomizedTimezone() throws CsvReaderException {
        final String input =
                "" + "Values\n" + "2021-09-27T19:00:00 UTC\n" + "\n" + "2021-09-27T20:00:00 UTC\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues(
                                "Values", 1632769200000000000L, Sentinels.NULL_LONG, 1632772800000000000L)
                                .reinterpret(Instant.class)
                                .reinterpret(Instant.class));

        // Simple custom time zone parser that only understands " UTC"
        Tokenizer.CustomTimeZoneParser myTimeZoneParser =
                (bs, tzo, off) -> {
                    if (bs.size() < 4) {
                        return false;
                    }
                    final byte[] d = bs.data();
                    final int o = bs.begin();
                    if (d[o] == ' ' && d[o + 1] == 'U' && d[o + 2] == 'T' && d[o + 3] == 'C') {
                        tzo.setValue(ZoneOffset.UTC);
                        off.setValue(0);
                        bs.setBegin(bs.begin() + 4);
                        return true;
                    }
                    return false;
                };

        invokeTest(defaultCsvBuilder().customTimeZoneParser(myTimeZoneParser).build(), input, expected);
    }

    private static final String ALL_NULLS = "" + "Values\n" + "\n" + "\n" + "\n" + "\n" + "\n";

    @Test
    public void unparseable() {
        final String input = "" + "Values\n" + "hello\n" + "there\n";

        Assertions.assertThatThrownBy(
                () -> invokeTest(
                        defaultCsvBuilder().parsers(Arrays.asList(Parsers.INT, Parsers.LONG, Parsers.DATETIME)).build(),
                        input,
                        ColumnSet.NONE));
    }

    @Test
    public void noParsers() {
        final String input = "" + "Values\n" + "hello\n" + "there\n";

        Assertions.assertThatThrownBy(
                () -> invokeTest(defaultCsvBuilder().parsers(Collections.emptyList()).build(), input, ColumnSet.NONE))
                .hasRootCauseMessage("No available parsers.");
    }

    @Test
    public void allNullsWithSpecifiedParser() throws CsvReaderException {
        final long nv = Sentinels.NULL_LONG;
        final ColumnSet expected = ColumnSet.of(Column.ofValues("Values", nv, nv, nv, nv, nv));

        invokeTest(defaultCsvBuilder().putParserForName("Values", Parsers.LONG).build(), ALL_NULLS, expected);
    }

    @Test
    public void allNullsWithNullParser() throws CsvReaderException {
        final long nv = Sentinels.NULL_LONG;
        final ColumnSet expected = ColumnSet.of(Column.ofValues("Values", nv, nv, nv, nv, nv));

        invokeTest(defaultCsvBuilder().nullParser(Parsers.LONG).build(), ALL_NULLS, expected);
    }

    @Test
    public void allNullsButNoParser() {
        Assertions
                .assertThatThrownBy(
                        () -> invokeTest(defaultCsvBuilder().nullParser(null).build(), ALL_NULLS, ColumnSet.NONE))
                .hasRootCauseMessage(
                        "Column contains all null cells, so can't infer type of column, and nullParser is not specified.");
    }

    @Test
    public void emptyTableWithSpecifiedParser() throws CsvReaderException {
        final String input = "Values\n";
        final ColumnSet expected = ColumnSet.of(Column.ofArray("Values", new long[0], 0));

        invokeTest(defaultCsvBuilder().putParserForName("Values", Parsers.LONG).build(), input, expected);
    }

    @Test
    public void unicode() throws CsvReaderException {
        final String input =
                "" + "Emojis\n" + "Hello 💖\n" + "Regular ASCII\n" + "✨ Deephaven ✨\n" + "🎆🎆🎆🎆🎆\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Emojis", "Hello 💖", "Regular ASCII", "✨ Deephaven ✨", "🎆🎆🎆🎆🎆"));

        invokeTest(defaultCsvSpecs(), input, expected);
    }

    /**
     * Test that input will be parsed as a char so long as it is in the BMP. The input is "tricky" because it starts out
     * looking like integrals.
     */
    @Test
    public void unicodeChars() throws CsvReaderException {
        // So long as a character is in the BMP (i.e. <= U+FFFF), it will be parsed as a char column.
        final String input =
                "" + "BMPChar\n" + "1\n" + "2\n" + "3\n" + "X\n" + "✈\n" + "❎\n" + "➉\n" + "✈\n" + "✨\n";

        final ColumnSet expected =
                ColumnSet.of(Column.ofValues("BMPChar", '1', '2', '3', 'X', '✈', '❎', '➉', '✈', '✨'));

        invokeTest(defaultCsvSpecs(), input, expected);
    }

    /** Large cells (10K characters or so), some with fancy Unicode, quotes, and escaped quotes. */
    @Test
    public void largeCells() throws CsvReaderException {
        final StringBuilder sbBytes = new StringBuilder();
        final StringBuilder sbChars = new StringBuilder();
        final StringBuilder sbQuotesEscaped = new StringBuilder();
        final StringBuilder sbQuotesLiteral = new StringBuilder();
        for (int ii = 0; ii < 1000; ++ii) {
            sbBytes.append("Deephaven!");
            sbChars.append("🍣Deep🍔haven!🍕");
            sbQuotesEscaped.append("Deep\"\"haven!");
            sbQuotesLiteral.append("Deep\"haven!");
        }
        final String largeCellBytes = sbBytes.toString();
        final String largeCellChars = sbChars.toString();
        final String largeCellEscaped = '"' + sbQuotesEscaped.toString() + '"';
        final String largeCellLiteral = sbQuotesLiteral.toString();

        final String input =
                ""
                        + "LargeEmojis\n"
                        + largeCellBytes
                        + "\n"
                        + largeCellChars
                        + "\n"
                        + largeCellEscaped
                        + "\n"
                        + largeCellBytes
                        + "\n"
                        + largeCellChars
                        + "\n"
                        + largeCellEscaped
                        + "\n";

        System.out.println(input);
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs(
                                "LargeEmojis",
                                largeCellBytes,
                                largeCellChars,
                                largeCellLiteral,
                                largeCellBytes,
                                largeCellChars,
                                largeCellLiteral));

        invokeTest(defaultCsvSpecs(), input, expected);
    }

    /** Test the global null literal value. */
    @Test
    public void customGlobalNullValue() throws CsvReaderException {
        final String input =
                ""
                        + "SomeBytes,SomeShorts,SomeInts,SomeLongs\n"
                        + "1,2,3,4\n"
                        + "NULL,NULL,NULL,NULL\n"
                        + "100,32000,2000000000,4000000000\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues("SomeBytes", (byte) 1, Sentinels.NULL_BYTE, (byte) 100),
                        Column.ofValues("SomeShorts", (short) 2, Sentinels.NULL_SHORT, (short) 32000),
                        Column.ofValues("SomeInts", 3, Sentinels.NULL_INT, 2000000000),
                        Column.ofValues("SomeLongs", 4L, Sentinels.NULL_LONG, 4000000000L));

        invokeTest(
                defaultCsvBuilder().parsers(Parsers.COMPLETE).nullValueLiterals(Collections.singletonList("NULL"))
                        .build(),
                input,
                expected);
    }

    /**
     * Test column-specific null literals values which may be specified by column name or index, and also show that
     * Unicode characters work as the null literal.
     */
    @Test
    public void customColumnSpecificNullValue() throws CsvReaderException {
        final String input =
                ""
                        + "SomeBytes,SomeShorts,SomeInts,SomeLongs\n"
                        + "1,2,3,4\n"
                        + "❌,🔥,⋰⋱,𝓓𝓮𝓮𝓹𝓱𝓪𝓿𝓮𝓷\n"
                        + "100,32000,2000000000,4000000000\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues("SomeBytes", (byte) 1, Sentinels.NULL_BYTE, (byte) 100),
                        Column.ofValues("SomeShorts", (short) 2, Sentinels.NULL_SHORT, (short) 32000),
                        Column.ofValues("SomeInts", 3, Sentinels.NULL_INT, 2000000000),
                        Column.ofValues("SomeLongs", 4L, Sentinels.NULL_LONG, 4000000000L));

        invokeTest(
                defaultCsvBuilder()
                        .parsers(Parsers.COMPLETE)
                        .putNullValueLiteralsForIndex(0, Collections.singletonList("❌"))
                        .putNullValueLiteralsForIndex(1, Collections.singletonList("🔥"))
                        .putNullValueLiteralsForName("SomeInts", Collections.singletonList("⋰⋱"))
                        .putNullValueLiteralsForName("SomeLongs", Collections.singletonList("𝓓𝓮𝓮𝓹𝓱𝓪𝓿𝓮𝓷"))
                        .build(),
                input,
                expected);
    }

    /** Provide a number of rows larger than ParserBase.DEST_BLOCK_SIZE. */
    @Test
    public void manyRows() throws CsvReaderException {
        final StringBuilder sb = new StringBuilder();
        sb.append(
                "SomeBooleans,SomeBytes,SomeShorts,SomeInts,SomeLongs,SomeDoubles,SomeStrings,SomeChars,SomeDateTimes,SomeTimestamps\n");
        final TByteArrayList booleans = new TByteArrayList();
        final TByteArrayList bytes = new TByteArrayList();
        final TShortArrayList shorts = new TShortArrayList();
        final TIntArrayList ints = new TIntArrayList();
        final TLongArrayList longs = new TLongArrayList();
        final TDoubleArrayList doubles = new TDoubleArrayList();
        final ArrayList<String> strings = new ArrayList<>();
        final TCharArrayList chars = new TCharArrayList();
        final TLongArrayList dateTimesAsLongs = new TLongArrayList();
        final TLongArrayList timestampsAsLongs = new TLongArrayList();
        final String qq = "qq";
        final long dtl = 799402088000000000L; // 1995-05-02 08:08:08Z
        final long tsl = 3456789012L;
        // Make sure we have a few more rows than Parser.DEST_BLOCK_SIZE
        for (int ii = 0; ii < Parser.CHUNK_SIZE + 3; ++ii) {
            sb.append("true,5,6,7,8,1.1,qq,r,1995-05-02 08:08:08Z,3456789012\n");
            booleans.add((byte) 1);
            bytes.add((byte) 5);
            shorts.add((short) 6);
            ints.add(7);
            longs.add(8);
            doubles.add(1.1);
            strings.add(qq);
            chars.add('r');
            dateTimesAsLongs.add(dtl);
            timestampsAsLongs.add(tsl);
        }
        // Add a row like this somewhere (let's put it at the end to make things challenging) so
        // inference picks the
        // right types.
        sb.append("false,100,32000,2000000000,4000000000,6.6e50,yy,z,2020-03-05 12:34:56Z,123456789\n");
        booleans.add((byte) 0);
        bytes.add((byte) 100);
        shorts.add((short) 32000);
        ints.add(2000000000);
        longs.add(4000000000L);
        doubles.add(6.6e50);
        strings.add("yy");
        chars.add('z');
        dateTimesAsLongs.add(1583411696000000000L); // 2020-03-05 12:34:56Z
        timestampsAsLongs.add(123456789);

        final String input = sb.toString();
        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofArray("SomeBooleans", booleans.toArray(), booleans.size()).reinterpret(boolean.class),
                        Column.ofArray("SomeBytes", bytes.toArray(), bytes.size()),
                        Column.ofArray("SomeShorts", shorts.toArray(), shorts.size()),
                        Column.ofArray("SomeInts", ints.toArray(), ints.size()),
                        Column.ofArray("SomeLongs", longs.toArray(), longs.size()),
                        Column.ofArray("SomeDoubles", doubles.toArray(), doubles.size()),
                        Column.ofArray("SomeStrings", strings.toArray(new String[0]), strings.size()),
                        Column.ofArray("SomeChars", chars.toArray(), chars.size()),
                        Column.ofArray("SomeDateTimes", dateTimesAsLongs.toArray(), dateTimesAsLongs.size())
                                .reinterpret(Instant.class),
                        Column.ofArray("SomeTimestamps", timestampsAsLongs.toArray(), timestampsAsLongs.size())
                                .reinterpret(Instant.class));
        invokeTest(
                defaultCsvBuilder()
                        .parsers(Parsers.COMPLETE)
                        .putParserForName("SomeTimestamps", Parsers.TIMESTAMP_NANOS)
                        .build(),
                input,
                expected);
    }

    @Test
    public void customParser() throws CsvReaderException {
        final String bd1 =
                "81290897538197389132106321892137218932178913227138932178912312132.21879213278912387692138723198";
        final String bd2 =
                "-9210381027382193791312718239712389127812931236183167913268912683921681293621891236821.12986178632478123678312762318";

        final String input = "" + "Index,BigValues\n" + "0," + bd1 + "\n" + "1,\n" + "2," + bd2 + "\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues("Index", 0, 1, 2),
                        Column.ofRefs("BigValues", new BigDecimal(bd1), null, new BigDecimal(bd2)));

        final MakeCustomColumn makeCustomColumn = (name, obj, size) -> {
            final BigDecimal[] arr = ((List<BigDecimal>) obj).toArray(new BigDecimal[0]);
            return Column.ofArray(name, arr, size);
        };
        invokeTest(defaultCsvBuilder().putParserForIndex(1, new MyBigDecimalParser()).build(), input, expected,
                makeMySinkFactory(), makeCustomColumn);
    }

    /**
     * Provide lots of data with a blackhole sink. System behaves reasonably (doesn't OOM).
     */
    @Test
    public void lotsOfDataDoesntChoke() throws CsvReaderException {
        final int numRows = 50_000_000;
        final RepeatingInputStream inputStream = new RepeatingInputStream("A,B\n", "111111111,222222222\n", numRows);

        final CsvSpecs specs = defaultCsvBuilder().parsers(Collections.singletonList(Parsers.INT)).build();
        final SinkFactory sinkFactory = makeBlackholeSinkFactory();
        final CsvReader.Result result = parse(specs, inputStream, sinkFactory);
    }

    /**
     * Check that Sinks get told their colnums.
     */
    @Test
    public void colnumPassedThrough() throws CsvReaderException {
        final String input = "" + "Col1,Col2,Col3\n" + "1,2,3\n" + "4,5,6\n" + "7,8,9\n";

        final InputStream inputStream = toInputStream(input);
        final CsvSpecs specs = defaultCsvSpecs();
        final SinkFactory sinkFactory = makeBlackholeSinkFactory();
        final CsvReader.Result result = parse(specs, inputStream, sinkFactory);
        final CsvReader.ResultColumn[] col = result.columns();
        final int bh0Num = (Integer) col[0].data();
        final int bh1Num = (Integer) col[1].data();
        final int bh2Num = (Integer) col[2].data();
        // 0-based column numbers
        Assertions.assertThat(bh0Num).isEqualTo(0);
        Assertions.assertThat(bh1Num).isEqualTo(1);
        Assertions.assertThat(bh2Num).isEqualTo(2);
    }

    /**
     * Addresses <a href="https://github.com/deephaven/deephaven-csv/issues/212"> A user requested that the library be
     * able to read files like this.
     */
    @Test
    public void bug212() throws CsvReaderException {
        final String input =
                ""
                        + "NAME                     STATUS       AGE      LABELS\n"
                        + "argo-events              Not Active   2y77d    app.kubernetes.io/instance=argo-events,kubernetes.io/metadata.name=argo-events\n"
                        + "argo-workflows           Active       2y77d    app.kubernetes.io/instance=argo-workflows,kubernetes.io/metadata.name=argo-workflows\n"
                        + "argocd                   Active       5y18d    kubernetes.io/metadata.name=argocd\n"
                        + "beta                     Not Active   4y235d   kubernetes.io/metadata.name=beta\n";

        final CsvSpecs specs = defaultCsvBuilder().hasFixedWidthColumns(true).build();

        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("NAME", "argo-events", "argo-workflows", "argocd", "beta"),
                Column.ofRefs("STATUS", "Not Active", "Active", "Active", "Not Active"),
                Column.ofRefs("AGE", "2y77d", "2y77d", "5y18d", "4y235d"),
                Column.ofRefs("LABELS",
                        "app.kubernetes.io/instance=argo-events,kubernetes.io/metadata.name=argo-events",
                        "app.kubernetes.io/instance=argo-workflows,kubernetes.io/metadata.name=argo-workflows",
                        "kubernetes.io/metadata.name=argocd",
                        "kubernetes.io/metadata.name=beta"));

        invokeTest(specs, input, expected);
    }

    /**
     * A basic test of fixed-width column support.
     */
    @Test
    public void simpleFixedColumnWidths() throws CsvReaderException {
        final String input =
                ""
                        + "Sym   Type     Price   SecurityId\n"
                        + "GOOG  Dividend 0.25    200\n"
                        + "T     Dividend 0.15    300\n"
                        + "Z     Dividend 0.18    500\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Sym", "GOOG", "T", "Z"),
                        Column.ofRefs("Type", "Dividend", "Dividend", "Dividend"),
                        Column.ofValues("Price", 0.25, 0.15, 0.18),
                        Column.ofValues("SecurityId", 200, 300, 500));

        final CsvSpecs specs =
                defaultCsvBuilder().hasFixedWidthColumns(true).build();

        invokeTest(specs, input, expected);
    }

    /**
     * We allow fixed-width data fields to fill the whole cell, without a padding character.
     */
    @Test
    public void fixedColumnWidthsFullCell() throws CsvReaderException {
        final String input =
                ""
                        + "Sym   Type     Price   SecurityId\n"
                        + "GOOGLEDividend!0.25    200\n"
                        + "T     Dividend 0.15    300\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Sym", "GOOGLE", "T"),
                        Column.ofRefs("Type", "Dividend!", "Dividend"),
                        Column.ofValues("Price", 0.25, 0.15),
                        Column.ofValues("SecurityId", 200, 300));

        final CsvSpecs specs =
                defaultCsvBuilder().hasFixedWidthColumns(true).build();
        invokeTest(specs, input, expected);
    }

    /**
     * Fixed-width cells can keep their padding characters or trim them, via {@link CsvSpecs#ignoreSurroundingSpaces}
     * Note that column headers themselves are always trimmed.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void fixedColumnsMayIncludeOrExcludeSurroundingSpaces(boolean ignoreSurroundingSpaces)
            throws CsvReaderException {
        final String input =
                ""
                        + "Sym   Type     Price   SecurityId\n"
                        + "GOOG  Dividend 0.25    200\n"
                        + "T     Dividend 0.15    300\n"
                        + "Z     Coupon   0.18    500\n";

        final String[] symData =
                ignoreSurroundingSpaces ? new String[] {"GOOG", "T", "Z"} : new String[] {"GOOG  ", "T     ", "Z     "};

        final String[] typeData = ignoreSurroundingSpaces ? new String[] {"Dividend", "Dividend", "Coupon"}
                : new String[] {"Dividend ", "Dividend ", "Coupon   "};


        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Sym", symData),
                        Column.ofRefs("Type", typeData),
                        Column.ofValues("Price", 0.25, 0.15, 0.18),
                        Column.ofValues("SecurityId", 200, 300, 500));

        final CsvSpecs specs =
                defaultCsvBuilder().hasFixedWidthColumns(true).ignoreSurroundingSpaces(ignoreSurroundingSpaces).build();

        invokeTest(specs, input, expected);
    }

    /**
     * Like delimited mode, fixed-width mode allows header rows to be skipped.
     */
    @Test
    public void fixedColumnWidthsSkipHeaderRows() throws CsvReaderException {
        final String input =
                ""
                        + "front matter\n"
                        + "ignore me\n"
                        + "Sym   Type     Price   SecurityId\n"
                        + "GOOG  Dividend 0.25    200\n"
                        + "T     Dividend 0.15    300\n"
                        + "Z     Dividend 0.18    500\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Sym", "GOOG", "T", "Z"),
                        Column.ofRefs("Type", "Dividend", "Dividend", "Dividend"),
                        Column.ofValues("Price", 0.25, 0.15, 0.18),
                        Column.ofValues("SecurityId", 200, 300, 500));

        final CsvSpecs specs =
                defaultCsvBuilder().hasFixedWidthColumns(true).skipHeaderRows(2).build();

        invokeTest(specs, input, expected);
    }

    /**
     * Like delimited mode, fixed-width mode allows data rows to be skipped.
     */
    @Test
    public void fixedColumnWidthsSkipDataRows() throws CsvReaderException {
        final String input =
                ""
                        + "Sym   Type     Price   SecurityId\n"
                        + "GOOG  Dividend 0.25    200\n"
                        + "T     Dividend 0.15    300\n"
                        + "XYZ1  Coupon   0.18    500\n"
                        + "XYZ2  Coupon   0.37    900\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Sym", "T", "XYZ1"),
                        Column.ofRefs("Type", "Dividend", "Coupon"),
                        Column.ofValues("Price", 0.15, 0.18),
                        Column.ofValues("SecurityId", 300, 500));

        // Skip 1 data row, take 2 data rows
        final CsvSpecs specs =
                defaultCsvBuilder().hasFixedWidthColumns(true).skipRows(1).numRows(2).build();

        invokeTest(specs, input, expected);
    }

    /**
     * Like delimited mode, fixed-width mode allows rows to be short.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void fixedColumnWidthsShortRows(boolean allowMissingColumns) throws CsvReaderException {
        final String input =
                ""
                        + "Sym   Type     Price   SecurityId\n"
                        + "GOOG\n"
                        + "T     Dividend 0.15    300\n"
                        + "Z     Dividend 0.18    500\n"
                        + "QQQ   Coupon\n";


        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Sym", "GOOG", "T", "Z", "QQQ"),
                        Column.ofRefs("Type", null, "Dividend", "Dividend", "Coupon"),
                        Column.ofValues("Price", Sentinels.NULL_DOUBLE, 0.15, 0.18, Sentinels.NULL_DOUBLE),
                        Column.ofValues("SecurityId", Sentinels.NULL_INT, 300, 500, Sentinels.NULL_INT));

        final CsvSpecs specs = defaultCsvBuilder().hasFixedWidthColumns(true)
                .allowMissingColumns(allowMissingColumns).build();

        if (allowMissingColumns) {
            invokeTest(specs, input, expected);
        } else {
            Assertions.assertThatThrownBy(() -> invokeTest(specs, input, expected))
                    .hasRootCauseMessage("Row 2 has too few columns (expected 4)");
        }
    }

    /**
     * Like delimited mode, fixed-width mode allows ignoring empty lines.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void fixedColumnWidthsIgnoreEmptyLines(boolean ignoreEmptyLines) throws CsvReaderException {
        final String input =
                ""
                        + "Sym   Type     Price   SecurityId\n"
                        + "GOOG  Dividend 0.25    200\n"
                        + "\n"
                        + "\n"
                        + "T     Dividend 0.15    300\n"
                        + "\n"
                        + "Z     Dividend 0.18    500\n";



        final ColumnSet expected;

        if (ignoreEmptyLines) {
            expected = ColumnSet.of(
                    Column.ofRefs("Sym", "GOOG", "T", "Z"),
                    Column.ofRefs("Type", "Dividend", "Dividend", "Dividend"),
                    Column.ofValues("Price", 0.25, 0.15, 0.18),
                    Column.ofValues("SecurityId", 200, 300, 500));
        } else {
            expected = ColumnSet.of(
                    Column.ofRefs("Sym", "GOOG", null, null, "T", null, "Z"),
                    Column.ofRefs("Type", "Dividend", null, null, "Dividend", null, "Dividend"),
                    Column.ofValues("Price", 0.25, Sentinels.NULL_DOUBLE, Sentinels.NULL_DOUBLE, 0.15,
                            Sentinels.NULL_DOUBLE, 0.18),
                    Column.ofValues("SecurityId", 200, Sentinels.NULL_INT, Sentinels.NULL_INT, 300, Sentinels.NULL_INT,
                            500));
        }

        final CsvSpecs specs = defaultCsvBuilder().hasFixedWidthColumns(true)
                .ignoreEmptyLines(ignoreEmptyLines).build();

        invokeTest(specs, input, expected);
    }

    /**
     * In fixed width mode, if there is no header row, the caller needs to specify column widths.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void noHeaderRowRequiresFixColumnWidthsSpecified(boolean specifyColumnWidths) throws CsvReaderException {
        final String input =
                ""
                        + "GOOG  Dividend 0.25    200\n"
                        + "T     Dividend 0.15    300\n"
                        + "Z     Coupon   0.18    500\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Column1", "GOOG", "T", "Z"),
                        Column.ofRefs("Column2", "Dividend", "Dividend", "Coupon"),
                        Column.ofValues("Column3", 0.25, 0.15, 0.18),
                        Column.ofValues("Column4", 200, 300, 500));

        final CsvSpecs.Builder specsBase = defaultCsvBuilder().hasFixedWidthColumns(true).hasHeaderRow(false);

        if (specifyColumnWidths) {
            final CsvSpecs specs = specsBase.fixedColumnWidths(Arrays.asList(6, 9, 8, 3)).build();
            invokeTest(specs, input, expected);
        } else {
            final CsvSpecs specs = specsBase.build();
            Assertions.assertThatThrownBy(() -> invokeTest(specs, input, expected))
                    .hasMessage("Can't proceed because hasHeaderRow is false but fixedColumnWidths is unspecified");
        }
    }

    /**
     * Because the library is tolerant of the last cell being shorter or wider than expected, the final entry in
     * fixedColumnWidths is just a placeholder.
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 5000, 34_000_000})
    public void finalFixedColumnWidthEntryIsPlaceholder(int finalEntry) throws CsvReaderException {
        final String input =
                ""
                        + "GOOG  Dividend 0.25    200\n"
                        + "T     Dividend 0.15    300\n"
                        + "Z     Coupon   0.18    500\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Column1", "GOOG", "T", "Z"),
                        Column.ofRefs("Column2", "Dividend", "Dividend", "Coupon"),
                        Column.ofValues("Column3", 0.25, 0.15, 0.18),
                        Column.ofValues("Column4", 200, 300, 500));

        final CsvSpecs.Builder specsBase = defaultCsvBuilder().hasFixedWidthColumns(true).hasHeaderRow(false);

        final CsvSpecs specs = specsBase.fixedColumnWidths(Arrays.asList(6, 9, 8, finalEntry)).build();
        invokeTest(specs, input, expected);
    }

    /**
     * Test all the parameters incompatible with delimited mode, all at the same time.
     */
    @Test
    public void checkParametersIncompatibleWithDelimitedMode() {
        final String expectedMessage =
                "CsvSpecs failed validation for the following reasons: " +
                        "Incompatible parameters: can't set fixedColumnWidths when hasFixedWidthColumns is false, " +
                        "Incompatible parameters: can't set useUtf32CountingConvention when hasFixedWidthColumns is false";

        Assertions.assertThatThrownBy(() -> defaultCsvBuilder().hasFixedWidthColumns(false)
                .useUtf32CountingConvention(false)
                .fixedColumnWidths(Arrays.asList(1, 2, 3, 4)).build()).hasMessage(expectedMessage);
    }

    /**
     * Test all the parameters incompatible with fixed-width mode, all at the same time.
     */
    @Test
    public void checkParametersIncompatibleWithFixedWidthMode() {
        final String expectedMessage =
                "CsvSpecs failed validation for the following reasons: " +
                        "Incompatible parameters: can't set quote when hasFixedWidthColumns is true, " +
                        "Incompatible parameters: can't set delimiter when hasFixedWidthColumns is true, " +
                        "Incompatible parameters: can't set trim when hasFixedWidthColumns is true";

        Assertions.assertThatThrownBy(() -> defaultCsvBuilder().hasFixedWidthColumns(true)
                .quote('X').delimiter('Y').trim(true).build()).hasMessage(expectedMessage);
    }


    /**
     * Test all the parameters incompatible with fixed-width mode, all at the same time.
     */
    @Test
    public void validateFixedWidthModeParameters() {
        final String expectedMessage =
                "CsvSpecs failed validation for the following reasons: " +
                        "Fixed column width -5 is invalid";

        Assertions.assertThatThrownBy(
                () -> defaultCsvBuilder().hasFixedWidthColumns(true).fixedColumnWidths(Arrays.asList(-5, 3, 8))
                        .build())
                .hasMessage(expectedMessage);
    }

    /**
     * In fixed width mode (as is also true in delimited mode), if there is no header row, the caller may specify column
     * names. If they don't, synthetic column names will be generated.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void columnNamesMayBeSpecified(boolean specifyColumnNames) throws CsvReaderException {
        final String input =
                ""
                        + "GOOG  Dividend 0.25    200\n"
                        + "T     Dividend 0.15    300\n"
                        + "Z     Coupon   0.18    500\n";

        final String[] expectedColumnNames = specifyColumnNames ? new String[] {"Sym", "Type", "Price", "SecurityId"}
                : new String[] {"Column1", "Column2", "Column3", "Column4"};

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs(expectedColumnNames[0], "GOOG", "T", "Z"),
                        Column.ofRefs(expectedColumnNames[1], "Dividend", "Dividend", "Coupon"),
                        Column.ofValues(expectedColumnNames[2], 0.25, 0.15, 0.18),
                        Column.ofValues(expectedColumnNames[3], 200, 300, 500));

        CsvSpecs.Builder specsBuilder = defaultCsvBuilder().hasFixedWidthColumns(true).hasHeaderRow(false)
                .fixedColumnWidths(Arrays.asList(6, 9, 8, 3));

        if (specifyColumnNames) {
            specsBuilder = specsBuilder.headers(Arrays.asList(expectedColumnNames));
        }

        invokeTest(specsBuilder.build(), input, expected);
    }

    /**
     * A counting convention test relevant to fixed-width mode. All six Unicode characters ♡♥❥❦◑╳ are in the Basic
     * Multilingual Plane and can all be represented with a single Java char. Therefore, they are counted the same with
     * both counting conventions.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void countsBMPCharactersTheSame(boolean useUtf32CountingConvention) throws CsvReaderException {
        final String input =
                ""
                        + "Sym   Type     Price   SecurityId\n"
                        + "♡♥❥❦◑╳Dividend 0.15    300\n"
                        + "Z     Dividend 0.18    500\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Sym", "♡♥❥❦◑╳", "Z"),
                        Column.ofRefs("Type", "Dividend", "Dividend"),
                        Column.ofValues("Price", 0.15, 0.18),
                        Column.ofValues("SecurityId", 300, 500));

        final CsvSpecs specs = defaultCsvBuilder().hasFixedWidthColumns(true)
                .useUtf32CountingConvention(useUtf32CountingConvention).build();

        invokeTest(specs, input, expected);
    }

    /**
     * A counting convention test relevant to fixed-width mode. All six Unicode characters 🥰😻🧡💓💕💖 are _outside_
     * the Basic Multilingual Plane and all are represented with two Java chars. The Sym column has a width of six. They
     * will fit in the "Sym" column if the caller uses the UTF-32 counting convention. They will not fit in the column
     * if the caller uses the UTF-16 counting convention (because it takes 12 Java chars to express them).
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void countsNonBMPCharactersDifferently(boolean useUtf32CountingConvention) throws CsvReaderException {
        final String input =
                ""
                        + "Sym   Type\n"
                        + "🥰😻🧡💓💕💖Dividend\n"
                        + "Z     Dividend\n";

        final ColumnSet expected;

        if (useUtf32CountingConvention) {
            expected = ColumnSet.of(
                    Column.ofRefs("Sym", "🥰😻🧡💓💕💖", "Z"),
                    Column.ofRefs("Type", "Dividend", "Dividend"));
        } else {
            expected = ColumnSet.of(
                    Column.ofRefs("Sym", "🥰😻🧡", "Z"),
                    Column.ofRefs("Type", "💓💕💖Dividend", "Dividend"));
        }

        final CsvSpecs specs = defaultCsvBuilder().hasFixedWidthColumns(true)
                .useUtf32CountingConvention(useUtf32CountingConvention).build();

        invokeTest(specs, input, expected);
    }

    /**
     * Using Unicode characters as column headers in fixed-width mode. We give one column a header with characters from
     * outside the BMP, and one with characters inside the BMP and show how the behavior differs depending on the
     * useUtf32CountingConvention flag. The header 🥰😻🧡 plus trailing space will be counted as width 4 in the UTF-32
     * counting convention, but width 7 in the UTF-16 column convention. Meanwhile, the header ╔═╤═╗ is counted as width
     * 5 in both conventions.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void unicodeColumnHeaders(boolean useUtf32CountingConvention) throws CsvReaderException {
        // In the UTF-32 counting convention, this is a column of width 4 (three Unicode characters plus the space)
        // followed by a column of width 5. The first cell of the data would therefore be "abc", and the next cell
        // would be "def".

        // In the UTF-16 counting convention, this is a column of width 7 (six UTF-16 units plus the space)
        // followed by a column of width 5. The first cell of the data would therefore be "abc def" and the next
        // cell woult be "gh".
        final String input =
                ""
                        + "🥰😻🧡 ╔═╤═╗\n"
                        + "abc defgh\n";

        final ColumnSet expected;

        if (useUtf32CountingConvention) {
            expected = ColumnSet.of(
                    Column.ofRefs("🥰😻🧡", "abc"),
                    Column.ofRefs("╔═╤═╗", "defgh"));
        } else {
            expected = ColumnSet.of(
                    Column.ofRefs("🥰😻🧡", "abc def"),
                    Column.ofRefs("╔═╤═╗", "gh"));
        }

        final CsvSpecs specs = defaultCsvBuilder().hasFixedWidthColumns(true)
                .useUtf32CountingConvention(useUtf32CountingConvention).build();

        invokeTest(specs, input, expected);
    }

    /**
     * In fixed-width mode, if the library is configured for the UTF-16 counting convention, and there is only one unit
     * of space left in the field, and the next character is a character outside the Basic Multilingual Plane that
     * requires two units, the library will include that character in the next field rather than this one.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void brokenSurrogatePair(boolean useUtf32CountingConvention) throws CsvReaderException {
        // This test has a column of width 3 (three characters plus the space)
        // followed by a column of width 2.
        //
        // In the UTF-32 counting convention, the first column will get "🥰😻 " and the second column will
        // get "🧡💓". We turn off ignoreSurroundingSpaces to highlight how this is counted.
        //
        // In the UTF-16 counting convention, the first column will get 🥰 (because 🥰😻 uses characters
        // outside the Basic Multilingual Plane and takes four units to represent, but the first field
        // only has space for three). The next column will get "😻 🧡💓" (the rest of the row).
        final String input =
                ""
                        + "C1 C2\n"
                        + "🥰😻 🧡💓\n";

        final ColumnSet expected;

        if (useUtf32CountingConvention) {
            expected = ColumnSet.of(
                    Column.ofRefs("C1", "🥰😻 "),
                    Column.ofRefs("C2", "🧡💓"));
        } else {
            expected = ColumnSet.of(
                    Column.ofRefs("C1", "🥰"),
                    Column.ofRefs("C2", "😻 🧡💓"));
        }

        final CsvSpecs specs = defaultCsvBuilder().hasFixedWidthColumns(true)
                .ignoreSurroundingSpaces(false).useUtf32CountingConvention(useUtf32CountingConvention).build();

        invokeTest(specs, input, expected);
    }

    private static final class RepeatingInputStream extends InputStream {
        private byte[] data;
        private final byte[] body;
        private int bodyRepeatsRemaining;
        private final byte[] tempBufferForRead;
        private int offset;

        public RepeatingInputStream(final String header, final String body, int bodyRepeats) {
            this.data = header.getBytes(StandardCharsets.UTF_8);
            this.body = body.getBytes(StandardCharsets.UTF_8);
            bodyRepeatsRemaining = bodyRepeats;
            tempBufferForRead = new byte[1];
            offset = 0;

        }

        @Override
        public int read() throws IOException {
            final int numBytes = read(tempBufferForRead, 0, 1);
            if (numBytes == 0) {
                return -1;
            }
            return tempBufferForRead[0] & 0xff;
        }

        @Override
        public int read(byte @NotNull [] b, int off, int len) {
            if (len == 0) {
                return 0;
            }
            while (offset == data.length) {
                if (bodyRepeatsRemaining == 0) {
                    return -1;
                }
                data = body;
                offset = 0;
                --bodyRepeatsRemaining;
            }
            final int currentSize = data.length - offset;
            final int sizeToUse = Math.min(currentSize, len);
            System.arraycopy(data, offset, b, off, sizeToUse);
            offset += sizeToUse;
            return sizeToUse;
        }
    }

    private static class MyBigDecimalParser implements Parser<BigDecimal[]> {
        @NotNull
        @Override
        public ParserContext<BigDecimal[]> makeParserContext(GlobalContext gctx, int chunkSize) {
            final MyBigDecimalSink sink = new MyBigDecimalSink();
            return new ParserContext<>(sink, null, DataType.CUSTOM, new BigDecimal[chunkSize]);
        }

        @Override
        public long tryParse(
                GlobalContext gctx,
                ParserContext<BigDecimal[]> pctx,
                IteratorHolder ih,
                long begin,
                long end,
                boolean appending)
                throws CsvReaderException {
            final boolean[] nulls = gctx.nullChunk();

            final Sink<BigDecimal[]> sink = pctx.sink();
            final BigDecimal[] values = pctx.valueChunk();

            // Reusable buffer
            char[] charData = new char[0];

            long current = begin;
            int chunkIndex = 0;
            do {
                if (chunkIndex == values.length) {
                    sink.write(values, nulls, current, current + chunkIndex, appending);
                    current += chunkIndex;
                    chunkIndex = 0;
                }
                if (current + chunkIndex == end) {
                    break;
                }
                if (gctx.isNullCell(ih)) {
                    nulls[chunkIndex++] = true;
                    continue;
                }
                final ByteSlice bs = ih.bs();
                if (!RangeTests.isAscii(bs.data(), bs.begin(), bs.end())) {
                    break;
                }

                // Convert bytes to chars. Annoying.
                if (charData.length < bs.size()) {
                    charData = new char[bs.size()];
                }
                int destIndex = 0;
                for (int cur = bs.begin(); cur != bs.end(); ++cur) {
                    charData[destIndex++] = (char) bs.data()[cur];
                }

                try {
                    values[chunkIndex] = new BigDecimal(charData, 0, destIndex);
                } catch (NumberFormatException ne) {
                    break;
                }
                nulls[chunkIndex] = false;
                ++chunkIndex;
            } while (ih.tryMoveNext());
            sink.write(values, nulls, current, current + chunkIndex, appending);
            return current + chunkIndex;
        }
    }

    private static class MyBigDecimalSink implements Sink<BigDecimal[]> {
        private final List<BigDecimal> dest = new ArrayList<>();

        @Override
        public void write(
                BigDecimal[] src, boolean[] isNull, long destBegin, long destEnd, boolean appending) {
            if (destBegin == destEnd) {
                return;
            }

            final int size = Math.toIntExact(destEnd - destBegin);
            if (appending) {
                // If the new area starts beyond the end of the destination, pad the destination.
                while (dest.size() < destBegin) {
                    dest.add(null);
                }
                for (int ii = 0; ii < size; ++ii) {
                    dest.add(isNull[ii] ? null : src[ii]);
                }
                return;
            }

            final int destBeginAsInt = Math.toIntExact(destBegin);
            for (int ii = 0; ii < size; ++ii) {
                dest.set(destBeginAsInt + ii, isNull[ii] ? null : src[ii]);
            }
        }

        @Override
        public Object getUnderlying() {
            return dest;
        }
    }

    private static final class ColumnSet {
        public static final ColumnSet NONE = new ColumnSet(new Column[0], 0);

        private final Column<?>[] columns;
        private final int columnSize;

        public static ColumnSet of(Column<?>... columns) {
            if (columns.length == 0) {
                throw new RuntimeException("Empty column set is not permitted");
            }
            final int c0Size = columns[0].size();
            for (int ii = 1; ii < columns.length; ++ii) { // Deliberately starting at 1.
                final int ciiSize = columns[ii].size();
                if (ciiSize != c0Size) {
                    throw new RuntimeException(
                            String.format(
                                    "Column %d (size %d) has a different size than column 0 (size %d)",
                                    ii, ciiSize, c0Size));
                }
            }
            return new ColumnSet(columns, c0Size);
        }

        private ColumnSet(Column<?>[] columns, int columnSize) {
            this.columns = columns;
            this.columnSize = columnSize;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            final List<Column<?>> colList = Arrays.asList(columns);

            final BiFunction<Class<?>, Class<?>, String> renderType =
                    (etype, rtype) -> {
                        if (etype == rtype) {
                            return etype.getCanonicalName();
                        }
                        return etype.getCanonicalName() + "->" + rtype.getCanonicalName();
                    };

            Renderer.renderList(
                    sb,
                    colList,
                    ",",
                    col -> String.format(
                            "%s(%s)",
                            col.name(), renderType.apply(col.elementType(), col.reinterpretedType())));
            for (int jj = 0; jj < columnSize; ++jj) {
                final int jjFinal = jj;
                sb.append('\n');
                Renderer.renderList(sb, colList, ",", col -> safeToString(col.getItem(jjFinal)));
            }
            return sb.toString();
        }

        private static String safeToString(Object o) {
            return o == null ? "(null)" : o.toString();
        }
    }

    private static final class Column<TARRAY> {
        private final String name;
        private final TARRAY values;
        private final int size;
        private final Class<?> reinterpretedType;

        public static Column<byte[]> ofValues(final String name, final byte... values) {
            return ofArray(name, values, values.length);
        }

        public static Column<short[]> ofValues(final String name, final short... values) {
            return ofArray(name, values, values.length);
        }

        public static Column<int[]> ofValues(final String name, final int... values) {
            return ofArray(name, values, values.length);
        }

        public static Column<long[]> ofValues(final String name, final long... values) {
            return ofArray(name, values, values.length);
        }

        public static Column<float[]> ofValues(final String name, final float... values) {
            return ofArray(name, values, values.length);
        }

        public static Column<double[]> ofValues(final String name, final double... values) {
            return ofArray(name, values, values.length);
        }

        public static Column<char[]> ofValues(final String name, final char... values) {
            return ofArray(name, values, values.length);
        }

        public static <T> Column<T[]> ofRefs(final String name, final T... values) {
            return ofArray(name, values, values.length);
        }

        public static <TARRAY> Column<TARRAY> ofArray(final String name, final TARRAY values, int size) {
            return new Column<>(name, values, size);
        }

        private Column(final String name, final TARRAY values, int size) {
            this(name, values, size, values.getClass().getComponentType());
        }

        private Column(final String name, final TARRAY values, int size, Class<?> reinterpretedType) {
            this.name = name;
            this.values = values;
            this.size = size;
            this.reinterpretedType = reinterpretedType;
        }

        public Column<TARRAY> reinterpret(Class<?> reinterpretedType) {
            return new Column<>(name, values, size, reinterpretedType);
        }

        public int size() {
            return size;
        }

        public String name() {
            return name;
        }

        public Class<?> elementType() {
            return values.getClass().getComponentType();
        }

        public Class<?> reinterpretedType() {
            return reinterpretedType;
        }

        public Object getItem(int index) {
            return Array.get(values, index);
        }
    }

    private static CsvSpecs defaultCsvSpecs() {
        return defaultCsvBuilder().build();
    }

    private static CsvSpecs.Builder defaultCsvBuilder() {
        return CsvSpecs.builder().ignoreSurroundingSpaces(true).allowMissingColumns(true);
    }

    private static void invokeTest(final CsvSpecs specs, final String input, final ColumnSet expected)
            throws CsvReaderException {
        invokeTest(specs, input, expected, makeMySinkFactory(), null);
    }

    private static void invokeTest(final CsvSpecs specs, final String input, final ColumnSet expected,
            final SinkFactory sinkFactory, MakeCustomColumn makeCustomColumn)
            throws CsvReaderException {
        invokeTest(specs, toInputStream(input), expected, sinkFactory, makeCustomColumn);
    }

    private static void invokeTest(final CsvSpecs specs, final InputStream inputStream, final ColumnSet expected,
            final SinkFactory sinkFactory, MakeCustomColumn makeCustomColumn)
            throws CsvReaderException {
        final CsvReader.Result result = parse(specs, inputStream, sinkFactory);
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
    private static CsvReader.Result parse(final CsvSpecs specs, final InputStream inputStream)
            throws CsvReaderException {
        return parse(specs, inputStream, makeMySinkFactory());
    }

    /**
     * Parses {@code inputStream} according to the specifications of {@code csvReader}.
     *
     *
     * @param inputStream the input stream.
     * @return The parsed data
     * @throws CsvReaderException If any sort of failure occurs.
     */
    private static CsvReader.Result parse(final CsvSpecs specs, final InputStream inputStream,
            final SinkFactory sinkFactory)
            throws CsvReaderException {
        return CsvReader.read(specs, inputStream, sinkFactory);
    }

    /** Convert String to InputStream */
    private static InputStream toInputStream(final String input) {
        final StringReader reader = new StringReader(input);
        return new ReaderInputStream(reader, StandardCharsets.UTF_8);
    }

    /***
     * Converts the {@link CsvReader.Result} to a {@link ColumnSet}.
     */
    private static ColumnSet toColumnSet(final CsvReader.Result result, MakeCustomColumn makeCustomColumn) {
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

    private interface MakeCustomColumn {
        Column<?> apply(String name, Object column, int size);
    }

    private static SinkFactory makeMySinkFactory() {
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

    private static SinkFactory makeBlackholeSinkFactory() {
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

    private static class Blackhole<TARRAY> implements Sink<TARRAY>, Source<TARRAY> {
        private final int colNum;

        public Blackhole(int colNum) {
            this.colNum = colNum;
        }

        @Override
        public void write(TARRAY src, boolean[] isNull, long destBegin, long destEnd, boolean appending) {
            // Do nothing.
        }

        /**
         * For the sake of one of our unit tests, we return the colNum as our underlying.
         */
        @Override
        public Object getUnderlying() {
            return colNum;
        }

        @Override
        public void read(TARRAY dest, boolean[] isNull, long srcBegin, long srcEnd) {
            // Do nothing.
        }
    }

    private static SinkFactory makeBlackholeSinkFactoryWithFailingDoubleSink() {
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

    private static SinkFactory makeBlackholeSinkFactoryWithSynchronizingDoubleSink(final int numParticipatingColumns,
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

    private static class FailingSink<TARRAY> implements Sink<TARRAY>, Source<TARRAY> {
        private final int colNum;

        public FailingSink(int colNum) {
            this.colNum = colNum;
        }

        @Override
        public void write(TARRAY src, boolean[] isNull, long destBegin, long destEnd, boolean appending) {
            throw new RuntimeException("synthetic error for testing: out of memory");
        }

        /**
         * For the sake of one of our unit tests, we return the colNum as our underlying.
         */
        @Override
        public Object getUnderlying() {
            return colNum;
        }

        @Override
        public void read(TARRAY dest, boolean[] isNull, long srcBegin, long srcEnd) {
            // Do nothing.
        }
    }

    private static class SynchronizingSink<TARRAY> implements Sink<TARRAY>, Source<TARRAY> {
        private final int colNum;
        private final SyncState syncState;
        private long sizeWritten = 0;

        public SynchronizingSink(int colNum, SyncState syncState) {
            this.colNum = colNum;
            this.syncState = syncState;
        }

        @Override
        public void write(TARRAY src, boolean[] isNull, long destBegin, long destEnd, boolean appending) {
            sizeWritten += destEnd - destBegin;
            syncState.maybeWait(colNum, sizeWritten);
        }

        @Override
        public Object getUnderlying() {
            return null;
        }

        @Override
        public void read(TARRAY dest, boolean[] isNull, long srcBegin, long srcEnd) {
            // Do nothing.
        }
    }

    private static class SyncState {
        private final int numParticipatingColumns;
        private final long thresholdSize;
        private long nextThreshold = 0;
        private int numWaiters = 0;

        public SyncState(int numParticipatingColumns, long thresholdSize) {
            this.numParticipatingColumns = numParticipatingColumns;
            this.thresholdSize = thresholdSize;
        }

        public synchronized void maybeWait(final int colNum, final long sizeWritten) {
            while (true) {
                if (sizeWritten < nextThreshold) {
                    return;
                }

                if (numWaiters == numParticipatingColumns - 1) {
                    // Everyone else is waiting, so advance the threshold.
                    nextThreshold += thresholdSize;
                    notifyAll();
                    continue;
                }

                ++numWaiters;
                try {
                    wait();
                } catch (InterruptedException ie) {
                    throw new RuntimeException("Interrupted", ie);
                } finally {
                    --numWaiters;
                }
            }
        }
    }

    private static String repeat(String x, int count) {
        return Stream.generate(() -> x).limit(count).collect(Collectors.joining());
    }
}
