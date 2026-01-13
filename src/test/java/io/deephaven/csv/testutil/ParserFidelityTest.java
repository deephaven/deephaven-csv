package io.deephaven.csv.testutil;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.parsers.Parser;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.util.CsvReaderException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.Collections;
import java.util.stream.Stream;


public class ParserFidelityTest {
    public enum ParseFidelity {
        /**
         * The CSV Parser and the Java Parser return exactly the same value
         */
        SAME,

        /**
         * The CSV Parser and the Java Parser return different values
         */
        DIFFERENT,

        /**
         * The CSV Parser throws an exception
         */
        PARSE_FAIL
    }

    private static Stream<Arguments> provideTuplesForTestFidelity() {
        // Tuple format is: input, strictFloatFidelity, fastFloatFidelity, doubleFidelity

        // Reminder:
        // FloatStrictParser uses Float.parseFloat
        // FloatFastParser uses a fast double parser (equivalent to Double.parseDouble) then casts to float
        // DoubleParser uses a fast double parser (equivalent to Double.parseDouble)
        return Stream.of(
                // CSV Parsers will agree with Java parsers
                Arguments.of("NaN", ParseFidelity.SAME, ParseFidelity.SAME, ParseFidelity.SAME),
                // CSV Parsers will agree with Java parsers
                Arguments.of("0", ParseFidelity.SAME, ParseFidelity.SAME, ParseFidelity.SAME),
                // CSV Parsers will agree with Java parsers
                Arguments.of("-0.0", ParseFidelity.SAME, ParseFidelity.SAME, ParseFidelity.SAME),
                // CSV Parsers will agree with Java parsers
                Arguments.of("+0.0", ParseFidelity.SAME, ParseFidelity.SAME, ParseFidelity.SAME),
                // CSV Parsers will agree with Java parsers
                Arguments.of("-Infinity", ParseFidelity.SAME, ParseFidelity.SAME, ParseFidelity.SAME),
                // CSV Parsers will agree with Java parsers
                Arguments.of("+Infinity", ParseFidelity.SAME, ParseFidelity.SAME, ParseFidelity.SAME),
                // CSV Parsers will agree with Java parsers
                Arguments.of("0.1", ParseFidelity.SAME, ParseFidelity.SAME, ParseFidelity.SAME),
                // FloatStrictParser will agree with Float.parseFloat
                // FloatFastParser will fail because Double.parseDouble yields a number on this input that is a tad
                // larger than Float.MAX_VALUE,
                // so the FloatFastParser will refuse it (and there is no type inference to double in this test).
                // DoubleParser will agree with Double.parseDouble
                Arguments.of("3.4028235E38", ParseFidelity.SAME, ParseFidelity.PARSE_FAIL, ParseFidelity.SAME),
                // CSV Parsers will agree with Java parsers
                Arguments.of("1.4E-45", ParseFidelity.SAME, ParseFidelity.SAME, ParseFidelity.SAME),
                // FloatStrictParser will agree with Float.parseFloat
                // FloatFastParser will succeed but disagree because (float)Double.parseDouble(input) yields a number
                // that is a tad different from Float.parseFloat(input)
                // DoubleParser will agree with Double.parseDouble
                Arguments.of("1.4798235515974676E-17", ParseFidelity.SAME, ParseFidelity.DIFFERENT,
                        ParseFidelity.SAME));
    }

    @ParameterizedTest
    @MethodSource("provideTuplesForTestFidelity")
    public void fidelity(String input,
            ParseFidelity strictFloatFidelity,
            ParseFidelity fastFloatFidelity,
            ParseFidelity doubleFidelity) {

        final Float expectedFloat = Float.parseFloat(input);
        final Double expectedDouble = Double.parseDouble(input);

        checkHelper(input, expectedFloat, Parsers.FLOAT_STRICT, strictFloatFidelity);
        checkHelper(input, expectedFloat, Parsers.FLOAT_FAST, fastFloatFidelity);
        checkHelper(input, expectedDouble, Parsers.DOUBLE, doubleFidelity);
    }

    private static void checkHelper(String input, Object expectedValue, Parser<?> parser,
            ParseFidelity fidelity) {
        final String source = "Values\n" + input + "\n";

        final CsvSpecs specs = CsvTestUtil.defaultCsvBuilder().parsers(Collections.singletonList(parser)).build();
        final InputStream stream = CsvTestUtil.toInputStream(source);
        CsvReader.Result result = null;
        boolean failed = false; // optimistically assume success
        try {
            result = CsvReader.read(specs, stream, CsvTestUtil.makeMySinkFactory());
        } catch (CsvReaderException e) {
            failed = true;
        }

        if (fidelity == ParseFidelity.PARSE_FAIL) {
            Assertions.assertThat(failed);
            return;
        }

        Assertions.assertThat(!failed);
        assert result != null; // make linter happy

        Assertions.assertThat(result.numCols()).isEqualTo(1);
        Assertions.assertThat(result.numRows()).isEqualTo(1);

        final Object array = result.columns()[0].data();
        final Object element0 = Array.get(array, 0);

        if (fidelity == ParseFidelity.SAME) {
            Assertions.assertThat(element0).isEqualTo(expectedValue);
        } else if (fidelity == ParseFidelity.DIFFERENT) {
            Assertions.assertThat(element0).isNotEqualTo(expectedValue);
        } else {
            throw new IllegalStateException();
        }
    }
}
