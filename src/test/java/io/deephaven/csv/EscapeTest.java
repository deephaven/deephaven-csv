package io.deephaven.csv;

import io.deephaven.csv.parsers.Parser;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.testutil.*;
import io.deephaven.csv.util.CsvReaderException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.stream.Stream;

public class EscapeTest {
    @ParameterizedTest
    @MethodSource("provideTuplesForEscapeTest")
    public void escapeTest(String input, Character escape, Object[] expectedValues) throws CsvReaderException {
        final Charset charset = StandardCharsets.UTF_8;
        final CsvSpecs specs = CsvTestUtil.defaultCsvBuilder().hasHeaderRow(false).quote('`').escape(escape).build();
        final InputStream stream = CsvTestUtil.toInputStream(input, charset);
        CsvReader.Result result = CsvReader.read(specs, stream, charset, CsvTestUtil.makeMySinkFactory());

        Assertions.assertThat(result.numRows()).isEqualTo(1);
        Assertions.assertThat(result.numCols()).isEqualTo(expectedValues.length);

        for (int i = 0; i != result.numCols(); ++i) {
            final Object array = result.columns()[i].data();
            final Object element0 = Array.get(array, 0);
            Assertions.assertThat(element0).isEqualTo(expectedValues[i]);
        }
    }

    private static Stream<Arguments> provideTuplesForEscapeTest() {
        // Note for Java readability we use unusual characters for quote and escape.
        // Namely: quote is ` (backtick) and escape is | (vertical bar).
        return Stream.of(
                // Cases that are not surrounded with quotes

                // Deep|,haven,42 with no escape configured comes through as "Deep|", "haven", 42
                Arguments.of("Deep|,haven,42\n", null, new Object[] {"Deep|", "haven", 42}),
                // Deep|,haven,42 with escape configured as | comes through as "Deep,haven", 42
                Arguments.of("Deep|,haven,42\n", '|', new Object[] {"Deep,haven", 42}),
                // Deephave|n,42 with no escape configured comes through as "Deephave|n", 42
                Arguments.of("Deephave|n,42\n", null, new Object[] {"Deephave|n", 42}),
                // Deephave|n,42 with escape configured as | comes through as "Deephave\n", with \n being newline
                Arguments.of("Deephave|n,42\n", '|', new Object[] {"Deephave\n", 42}),

                // Cases that are surrounded with quotes

                // `Deep,haven`,42 with no escape configured comes through as "Deep,haven", 42
                // because quotation marks are another way to escape the field separator.
                Arguments.of("`Deep,haven`,42\n", null, new Object[] {"Deep,haven", 42}),
                // `Deep|,haven`,42 with escape configured as | also comes through as "Deep,haven", 42
                // because the escape is processed even inside quotes.
                Arguments.of("`Deep|,haven`,42\n", '|', new Object[] {"Deep,haven", 42}),
                // `Deephave|n`,42 with no escape configured comes through as "Deephave|n", 42
                Arguments.of("`Deephave|n`,42\n", '*', new Object[] {"Deephave|n", 42}),
                // `Deephave|n,42` with escape configured as | comes through as "Deephave\n", with \n being newline
                Arguments.of("`Deephave|n`,42\n", '|', new Object[] {"Deephave\n", 42}),

                // C style escapes

                // Without escape configured, C-style escapes are not special
                Arguments.of("Deep|b|r|n|t|fhaven,42\n", null, new Object[] {"Deep|b|r|n|t|fhaven", 42}),
                // With escape configured, C-style escapes are special
                Arguments.of("Deep|b|r|n|t|fhaven,42\n", '|', new Object[] {"Deep\b\r\n\t\fhaven", 42}),
                // Surrounding with quotes, without escape configured, C-style escapes are not special
                Arguments.of("`Deep|b|r|n|t|fhaven`,42\n", null, new Object[] {"Deep|b|r|n|t|fhaven", 42}),
                // Surrounding with quotes, with escape configured, C-style escapes are special
                Arguments.of("`Deep|b|r|n|t|fhaven`,42\n", '|', new Object[] {"Deep\b\r\n\t\fhaven", 42}),

                // Quotation mark in the middle of unquoted text

                // Without escape configured: a quotation mark in the middle is passed through
                Arguments.of("Deep`haven,42\n", null, new Object[] {"Deep`haven", 42}),
                // With escape configured: a quotation mark in the middle is still passed through (not that interesting)
                Arguments.of("Deep`haven,42\n", '|', new Object[] {"Deep`haven", 42}),
                // Without escape configured: an escaped quotation mark in the middle just passes through the | and the
                // `
                Arguments.of("Deep|`haven,42\n", null, new Object[] {"Deep|`haven", 42}),
                // With escape configured: an escaped quotation mark in the middle passes through the `
                Arguments.of("Deep|`haven,42\n", '|', new Object[] {"Deep`haven", 42}),

                // Getting a quotation mark in the middle of quoted text

                // Without escape configured: a double quotation mark in the middle is passed through as a single quote
                Arguments.of("`Deep``haven`,42\n", null, new Object[] {"Deep`haven", 42}),
                // With escape configured: a double quotation mark in the middle is still passed through (same as above)
                Arguments.of("`Deep``haven`,42\n", '|', new Object[] {"Deep`haven", 42}),
                // With escape configured: a single escaped quotation mark in the middle passes through the quote
                Arguments.of("`Deep|`haven`,42\n", '|', new Object[] {"Deep`haven", 42}));
    }

    @ParameterizedTest
    @MethodSource("provideTuplesForQuoteTest")
    public void choiceOfQuoteTest(String input, char quote, Object[] expectedValues) throws CsvReaderException {
        final Charset charset = StandardCharsets.UTF_8;
        final CsvSpecs specs = CsvTestUtil.defaultCsvBuilder().hasHeaderRow(false).quote(quote).escape('|').build();
        final InputStream stream = CsvTestUtil.toInputStream(input, charset);
        CsvReader.Result result = CsvReader.read(specs, stream, charset, CsvTestUtil.makeMySinkFactory());

        Assertions.assertThat(result.numRows()).isEqualTo(1);
        Assertions.assertThat(result.numCols()).isEqualTo(expectedValues.length);

        for (int i = 0; i != result.numCols(); ++i) {
            final Object array = result.columns()[i].data();
            final Object element0 = Array.get(array, 0);
            Assertions.assertThat(element0).isEqualTo(expectedValues[i]);
        }
    }

    private static Stream<Arguments> provideTuplesForQuoteTest() {
        return Stream.of(
                // `Deep|`haven`,42 comes through as Deep`haven, 42
                Arguments.of("`Deep|`haven`,42\n", '`', new Object[] {"Deep`haven", 42}),
                // *Deep|*haven*,42 comes through as Deep*haven, 42
                Arguments.of("*Deep|*haven*,42\n", '*', new Object[] {"Deep*haven", 42}),
                // nDeephave|nn,42 comes through as Deephave\n, 42 where n is the newline.
                // This demonstrates the fanciful example that you *can* use 'n' as a quote character, but
                // you should know that \n will translate to newline, not escape your quote character.
                Arguments.of("nDeephave|nn,42\n", 'n', new Object[] {"Deephave\n", 42}),
                // nDeephavennn,42 comes through as Deephaven, 42.
                // Following up to the above, this shows if you use 'n' as a quote character
                // and you want it in your data, you need to double it.
                Arguments.of("nDeephavennn,42\n", 'n', new Object[] {"Deephaven", 42}));
    }

    @ParameterizedTest
    @MethodSource("provideTuplesForErroneousUseOfEscapeTest")
    public void erroneousUseOfEscape(String input, String exceptionFragment) throws CsvReaderException {
        final Charset charset = StandardCharsets.UTF_8;
        final CsvSpecs specs = CsvTestUtil.defaultCsvBuilder().hasHeaderRow(false).escape('|').build();
        final InputStream stream = CsvTestUtil.toInputStream(input, charset);

        Assertions.assertThatThrownBy(() -> {
            CsvReader.read(specs, stream, charset, CsvTestUtil.makeMySinkFactory());
        }).hasMessageContaining(exceptionFragment);
    }

    private static Stream<Arguments> provideTuplesForErroneousUseOfEscapeTest() {
        return Stream.of(
                // Last character of input cannot be escape
                Arguments.of("hello|", "The escape character cannot be the last character of the input"),
                // Cannot escape carriage return
                Arguments.of("hello|\r", "Can't escape a carriage return or newline"),
                // Cannot escape newline
                Arguments.of("hello|\n", "Can't escape a carriage return or newline"),
                // Cannot escape non-ASCII
                Arguments.of("hello|❤", "Can't escape a non-ASCII character"));
    }
}
