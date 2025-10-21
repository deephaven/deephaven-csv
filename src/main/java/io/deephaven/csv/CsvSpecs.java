package io.deephaven.csv;

import io.deephaven.csv.annotations.BuildableStyle;
import io.deephaven.csv.parsers.Parser;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.tokenization.JdkDoubleParser;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.tokenization.Tokenizer.CustomDoubleParser;
import io.deephaven.csv.util.Renderer;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A specification object for parsing CSV input.
 */
@Immutable
@BuildableStyle
public abstract class CsvSpecs {
    /**
     * The Builder for the CsvSpecs class.
     */
    public interface Builder {
        /**
         * Copy all the parameters from {@code specs} into {@code this} builder.
         * 
         * @param specs The source object
         * @return self after copying over all the properties.
         */
        Builder from(CsvSpecs specs);

        /**
         * Client-specified headers that can be used to override the existing headers in the input (if
         * {@link #hasHeaderRow()} is true), or to provide absent headers (if {@link #hasHeaderRow()} is false).
         * 
         * @param elements The client-specified headers
         * @return self after modifying the headers property.
         */
        Builder headers(Iterable<String> elements);

        /**
         * Override a specific column header by 0-based column index. This is applied after {@link #headers()}.
         * 
         * @param index The column index
         * @param header The column header
         * @return self after modifying the header property for the given column index.
         */
        Builder putHeaderForIndex(int index, String header);

        /**
         * The parsers that the user wants to participate in type inference. Note that the order that the parsers in
         * this list matters only for custom parsers. In particular:
         * <ol>
         * <li>Standard system parsers (singletons from the {@link Parsers} class) will run in their standard precedence
         * order, regardless of the order they appear here.</li>
         * <li>All specified system parsers will be run before any specified custom parsers.</li>
         * <li>Custom parsers will be run in the order they are specified here.</li>
         * </ol>
         * 
         * @param elements The parsers
         * @return self after modifying the parsers property.
         */
        Builder parsers(Iterable<? extends Parser<?>> elements);

        /**
         * Used to force a specific parser for a specific column, specified by column name. Specifying a parser forgoes
         * column inference for that column. If {@link #putParserForName} and {@link #putParserForIndex} both refer to
         * the same column, {@link #putParserForName} takes priority.
         * 
         * @param columnName The column name
         * @param parser The parser
         * @return self after modifying the parser property for the given columnName.
         */
        Builder putParserForName(String columnName, Parser<?> parser);

        /**
         * Used to force a specific parser for a specific column, specified by 0-based column index. Specifying a parser
         * forgoes column inference for that column. If {@link #putParserForName} and {@link #putParserForIndex} both
         * refer to the same column, {@link #putParserForName} takes priority.
         * 
         * @param index The column index
         * @param parser The parser
         * @return self after modifying the parser property for the given column index.
         */
        Builder putParserForIndex(int index, Parser<?> parser);

        /**
         * The default collection of strings that means "null value" in the input. These defaults are used for a column
         * if there is no corresponding {@link #nullValueLiteralsForName()} or {@link #nullValueLiteralsForIndex()}
         * specified for that column. Default value is a single-element list containing the empty string. If the
         * configured sink data structures do not support nulls, the caller can set this to the empty list so that
         * nothing will be parsed as null.
         * 
         * @param nullValueLiterals The collection of null value literal strings
         * @return self after modifying the nullValueLiterals property.
         */
        Builder nullValueLiterals(Iterable<String> nullValueLiterals);

        /**
         * The null value literal for specific columns, specified by column name. Specifying a null value literal for a
         * column overrides the value in {@link #nullValueLiterals()}. If {@link #putNullValueLiteralsForName} and
         * {@link #putNullValueLiteralsForIndex} both refer to the same column, {@link #putNullValueLiteralsForName}
         * takes priority.
         * 
         * @param columnName The column name
         * @param nullValueLiteral The collection of null value literal strings
         * @return self after modifying the nullValueLiteral property for the specified columnName.
         */
        Builder putNullValueLiteralsForName(String columnName, List<String> nullValueLiteral);

        /**
         * The null value literal for specific columns, specified by 0-based column index. Specifying a null value
         * literal for a column overrides the value in {@link #nullValueLiterals()}. If
         * {@link #putNullValueLiteralsForName} and {@link #putNullValueLiteralsForIndex} both refer to the same column,
         * {@link #putNullValueLiteralsForName} takes priority.
         * 
         * @param index The column index
         * @param nullValueLiteral The collection of null value literal strings
         * @return self after modifying the nullValueLiteral property for the specified column index.
         */
        Builder putNullValueLiteralsForIndex(int index, List<String> nullValueLiteral);

        /**
         * The parser to uses when all values in the column are null. The default is {@link Parsers#STRING}.
         * 
         * @param parser The parser
         * @return self after modifying the parser property.
         */
        Builder nullParser(Parser<?> parser);

        /**
         * The custom double parser. If not explicitly set, it will default to {@link CustomDoubleParser#load()} if
         * present, otherwise {@link JdkDoubleParser#INSTANCE}.
         * 
         * @param customDoubleParser The custom double parser
         * @return self after modifying the customDoubleParser property.
         */
        Builder customDoubleParser(Tokenizer.CustomDoubleParser customDoubleParser);

        /**
         * An optional low-level "timezone parser" that understands custom time zone strings. For example the Deephaven
         * system allows special timezones like " NY" and " MN" as in "2020-03-01T12:34:56 NY" (note also the explicit
         * space). The timezone parser must be reentrant.
         * 
         * @param customTimeZoneParser The custom timezone parser.
         * @return self after modifying the customTimeZoneParser property.
         */
        Builder customTimeZoneParser(Tokenizer.CustomTimeZoneParser customTimeZoneParser);

        /**
         * An optional legalizer for column headers. The legalizer is a function that takes the column names from the
         * input (as a {@code String[]}) and returns "legal" column names (as a {@code String[]}). What constitutes
         * "legal" is entirely up to the caller. For example, some applications cannot tolerate punctuation characters
         * in column names and need to remove them. The CSV library itself has no limitations with regard to column
         * names. The legalizer function is permitted to return the input array (perhaps with some elements modified) as
         * the return value. The default is {@link Function#identity()}.
         * 
         * @param headerLegalizer The custom header legalizer.
         * @return self after modifying the headerLegalizer property.
         */
        Builder headerLegalizer(Function<String[], String[]> headerLegalizer);

        /**
         * An optional validator for column headers. The validator is a {@link Predicate} function that takes a column
         * name and returns a true if it is a legal column name, false otherwise. The default is {@code c -> true}.
         * 
         * @param headerValidator The custom header validator.
         * @return self after modifying the headerValidator property.
         */
        Builder headerValidator(Predicate<String> headerValidator);

        /**
         * True if the input is organized into fixed width columns rather than delimited by a delimiter.
         * 
         * @param hasFixedWidthColumns The fixed width columns property
         * @return self after modifying the hasFixedWidthColumns property.
         */
        Builder hasFixedWidthColumns(boolean hasFixedWidthColumns);

        /**
         * When {@link #hasFixedWidthColumns} is set, the library either determines the column widths from the header
         * row (provided {@link #hasHeaderRow} is set), or the column widths can be specified explicitly by the caller.
         * If the caller wants to specify them explicitly, they can use this method. It is an error to set this
         * parameter if {@link #hasFixedWidthColumns} is false. Note that because the library is tolerant of the last
         * cell being shorter or wider than expected, the value specified here for the width of the last column is
         * simply a placeholder; its value is ignored.
         * 
         * @param fixedColumnWidths A collection of fixed column widths
         * @return self after modifying the fixedColumnWidths property.
         */
        Builder fixedColumnWidths(Iterable<Integer> fixedColumnWidths);

        /**
         * This setting controls what units fixed width columns are measured in. When true, fixed width columns are
         * measured in Unicode code points. When false, fixed width columns are measured in UTF-16 units (aka Java
         * chars). The difference arises when encountering characters outside the Unicode Basic Multilingual Plane. For
         * example, the Unicode code point 💔 (U+1F494) is one Unicode code point, but takes two Java chars to
         * represent. Along these lines, the string 💔💔💔 would fit in a column of width 3 when utf32CountingMode is
         * true, but would require a column width of at least 6 when utf32CountingMode is false. The setting of true is
         * arguably more natural for users (the number of characters they see matches the visual width of the column).
         * But some programs may want the value of false because they are counting Java chars. It is an error to set
         * this parameter if {@link #hasFixedWidthColumns} is false. The default is
         * {@value #defaultUtf32CountingConvention}.
         * 
         * @param useUtf32CountingConvention The useUtf32CountingConvention property.
         * @return self after modifying the useUtf32CountingConvention property.
         */
        Builder useUtf32CountingConvention(boolean useUtf32CountingConvention);

        /**
         * Number of data rows to skip before processing data. This is useful when you want to parse data in chunks.
         * Typically used together with {@link Builder#numRows}. The default is 0.
         * 
         * @param skipRows The skipRows property
         * @return self after modifying the skipRows property.
         */
        Builder skipRows(long skipRows);

        /**
         * Max number of rows to process. This is useful when you want to parse data in chunks. Typically used together
         * with {@link Builder#skipRows}. The default is {@link Long#MAX_VALUE}.
         * 
         * @param numRows The numRows property
         * @return self after modifying the numRows property.
         */
        Builder numRows(long numRows);

        /**
         * Whether the library should skip over empty lines in the input. The default is false.
         * 
         * @param ignoreEmptyLines the ignoreEmptyLines property
         * @return self after modifying the ignoreEmptyLines property.
         */
        Builder ignoreEmptyLines(boolean ignoreEmptyLines);

        /**
         * Whether the library should allow missing columns in the input. If this flag is set, then rows that are too
         * short (that have fewer columns than the header row) will be interpreted as if the missing columns contained
         * the empty string. The default is false.
         * 
         * @param allowMissingColumns The allowMissingColumns property
         * @return self after modifying the allowMissingColumns property
         */
        Builder allowMissingColumns(boolean allowMissingColumns);

        /**
         * Whether the library should allow excess columns in the input. If this flag is set, then rows that are too
         * long (that have more columns than the header row) will have those excess columns dropped. The default is
         * false.
         * 
         * @param ignoreExcessColumns The ignoreExcessColumns property
         * @return self after modifying the ignoreExcessColumns parameter
         */
        Builder ignoreExcessColumns(boolean ignoreExcessColumns);

        /**
         * Whether the input file has a header row. The default is true.
         * 
         * @param hasHeaderRow The hasHeaderRow property
         * @return self after modifying the hasHeaderRow property.
         */
        Builder hasHeaderRow(boolean hasHeaderRow);

        /**
         * Number of rows to skip before reading the header row from the input. Valid only if
         * {@link Builder#hasHeaderRow} is true. 0 means the header row is the first line of the file.
         * 
         * @param skipHeaderRows The skipHeaderRows property.
         * @return self after modifying the skipHeaderRows property.
         */
        Builder skipHeaderRows(long skipHeaderRows);

        /**
         * The field delimiter character (the character that separates one column from the next). Must be 7-bit ASCII.
         * The default is '{@value #defaultDelimiter}'. It is an error to set this parameter if
         * {@link #hasFixedWidthColumns} is true.
         * 
         * @param delimiter The deliminter property.
         * @return self after modifying the delimiter property.
         */
        Builder delimiter(char delimiter);

        /**
         * The quote character (used when you want field or line delimiters to be interpreted as literal text. Must be
         * 7-bit ASCII. The default is '{@value #defaultQuote}' For example:
         *
         * <pre>
         * 123,"hello, there",456,
         * </pre>
         *
         * Would be read as the three fields:
         *
         * <ul>
         * <li>123
         * <li>hello, there
         * <li>456
         * </ul>
         *
         * It is an error to set this parameter if {@link #hasFixedWidthColumns} is true.
         * 
         * @param quote The quote property.
         * @return self after modifying the quote property.
         */
        Builder quote(char quote);

        /**
         * Whether to trim leading and trailing blanks from non-quoted values. The default is {@code true}.
         * 
         * @param ignoreSurroundingSpaces The ignoreSurroundingSpaces property.
         * @return self after modifying the ignoreSurroundingSpaces property.
         */
        Builder ignoreSurroundingSpaces(boolean ignoreSurroundingSpaces);

        /**
         * Whether to trim leading and trailing blanks from inside quoted values. It is an error to set this parameter
         * if {@link #hasFixedWidthColumns} is true. The default is {@value #defaultTrim}.
         * 
         * @param trim The trim property.
         * @return self after modifying the trim property.
         */
        Builder trim(boolean trim);

        /**
         * Whether to run concurrently. In particular, the operation that reads the raw file, breaks it into columns,
         * and stores that column text in memory can run in parallel with the column parsers, and the parsers can run in
         * parallel with each other. The default is true.
         * 
         * @param async The async property.
         * @return self after modifying the concurrent property.
         */
        Builder concurrent(boolean async);

        /**
         * The duration to wait for all reader threads to shut down if processing ends abnormally, typically due to an
         * exception while reading data. The default is 1 minute.
         * 
         * @param timeout The timeout property.
         * @return self after modifying the timeout property.
         */
        Builder threadShutdownTimeout(Duration timeout);

        /**
         * Build the CsvSpecs object.
         * 
         * @return The built object.
         */
        CsvSpecs build();
    }

    /**
     * Creates a builder for {@link CsvSpecs}.
     * 
     * @return The builder.
     */
    public static Builder builder() {
        return ImmutableCsvSpecs.builder();
    }

    /**
     * Validates the {@link CsvSpecs}.
     */
    @Check
    void check() {
        // To be friendly, we report all the problems we find at once.
        final List<String> problems = new ArrayList<>();
        check7BitAscii("quote", quote(), problems);
        check7BitAscii("delimiter", delimiter(), problems);
        checkNonnegative("skipRows", skipRows(), problems);
        checkNonnegative("skipHeaderRows", skipHeaderRows(), problems);
        checkNonnegative("numRows", numRows(), problems);
        if (!hasHeaderRow() && skipHeaderRows() > 0) {
            problems.add("skipHeaderRows != 0 but hasHeaderRow is not set");
        }

        for (final Integer colWidth : fixedColumnWidths()) {
            if (colWidth < 1) {
                problems.add(String.format("Fixed column width %d is invalid", colWidth));
            }
        }

        // Certain items must not be set in fixed-width column mode. Other items must not be set in delimited column
        // mode.
        if (hasFixedWidthColumns()) {
            final String format = "Incompatible parameters: can't set %s when hasFixedWidthColumns is true";
            if (quote() != defaultQuote) {
                problems.add(String.format(format, "quote"));
            }

            if (delimiter() != defaultDelimiter) {
                problems.add(String.format(format, "delimiter"));
            }

            if (trim() != defaultTrim) {
                problems.add(String.format(format, "trim"));
            }
        } else {
            final String format = "Incompatible parameters: can't set %s when hasFixedWidthColumns is false";
            if (fixedColumnWidths().size() != 0) {
                problems.add(String.format(format, "fixedColumnWidths"));
            }

            if (useUtf32CountingConvention() != defaultUtf32CountingConvention) {
                problems.add(String.format(format, "useUtf32CountingConvention"));
            }
        }
        if (problems.isEmpty()) {
            return;
        }
        final String message = "CsvSpecs failed validation for the following reasons: " + Renderer.renderList(problems);
        throw new RuntimeException(message);
    }

    /**
     * A comma-separated-value delimited format.
     * 
     * @return The CsvSpecs for the specified format.
     */
    public static CsvSpecs csv() {
        return builder().build();
    }

    /**
     * A tab-separated-value delimited format. Equivalent to {@code builder().delimiter('\t').build()}.
     * 
     * @return The CsvSpecs for the specified format.
     */
    public static CsvSpecs tsv() {
        return builder().delimiter('\t').build();
    }

    /**
     * A header-less, CSV format. Equivalent to {@code builder().hasHeaderRow(false).build()}.
     * 
     * @return The CsvSpecs for the specified format.
     */
    public static CsvSpecs headerless() {
        return builder().hasHeaderRow(false).build();
    }

    /**
     * See {@link Builder#headers}.
     * 
     * @return The client-specified headers
     */
    public abstract List<String> headers();

    /**
     * See {@link Builder#putHeaderForIndex}
     * 
     * @return The client-specified header for a given index.
     */
    public abstract Map<Integer, String> headerForIndex();

    /**
     * See {@link Builder#parsers}.
     * 
     * @return The set of configured parsers.
     */
    @Default
    public List<Parser<?>> parsers() {
        return Parsers.DEFAULT;
    }

    /**
     * See {@link Builder#putParserForName}.
     * 
     * @return A map of the client-specified parsers, keyed by column name.
     */
    public abstract Map<String, Parser<?>> parserForName();

    /**
     * See {@link Builder#putParserForIndex}.
     * 
     * @return A map of the client-specified parsers, keyed by column index.
     */
    public abstract Map<Integer, Parser<?>> parserForIndex();

    /**
     * See {@link Builder#nullValueLiterals}.
     * 
     * @return The collection of strings that mean "null value".
     */
    @Default
    public List<String> nullValueLiterals() {
        return Collections.singletonList("");
    }

    /**
     * See {@link Builder#nullValueLiterals}.
     * 
     * @return The collection of strings that mean "null value", on a per-column-name basis.
     */
    public abstract Map<String, List<String>> nullValueLiteralsForName();

    /**
     * See {@link Builder#putNullValueLiteralsForIndex}.
     * 
     * @return The collection of strings that mean "null value", on a per-column-index basis.
     */
    public abstract Map<Integer, List<String>> nullValueLiteralsForIndex();

    /**
     * See {@link Builder#nullParser}.
     * 
     * @return The parser to use when all values in the column are null.
     */
    @Default
    @Nullable
    public Parser<?> nullParser() {
        return Parsers.STRING;
    }

    /**
     * See {@link Builder#customDoubleParser}.
     * 
     * @return The parser to use to parse doubles.
     */
    @Default
    public Tokenizer.CustomDoubleParser customDoubleParser() {
        return CustomDoubleParser.load().orElse(JdkDoubleParser.INSTANCE);
    }

    /**
     * See {@link Builder#customTimeZoneParser}.
     * 
     * @return The user-specified parser that understands additional time zone formats.
     */
    @Default
    @Nullable
    public Tokenizer.CustomTimeZoneParser customTimeZoneParser() {
        return null;
    }

    /**
     * See {@link Builder#headerLegalizer}.
     * 
     * @return The user-specified function that converts column names into an acceptable form.
     */
    @Default
    public Function<String[], String[]> headerLegalizer() {
        return Function.identity();
    }

    /**
     * See {@link Builder#headerValidator}.
     * 
     * @return The user-specified function that determines whether a column name is acceptable.
     */
    @Default
    public Predicate<String> headerValidator() {
        return c -> true;
    }

    /**
     * See {@link Builder#hasFixedWidthColumns}.
     * 
     * @return Whether the caller has configured fixed width columns
     */
    @Default
    public boolean hasFixedWidthColumns() {
        return false;
    }

    /**
     * See {@link Builder#fixedColumnWidths}.
     * 
     * @return Caller-specified fixed column widths.
     */
    @Default
    public List<Integer> fixedColumnWidths() {
        return Collections.emptyList();
    }

    private static final boolean defaultUtf32CountingConvention = true;

    /**
     * See {@link Builder#useUtf32CountingConvention}.
     * 
     * @return Whether the caller specified the UTF-32 counting convention.
     */
    @Default
    public boolean useUtf32CountingConvention() {
        return defaultUtf32CountingConvention;
    }

    /**
     * See {@link Builder#skipRows}.
     * 
     * @return Whether the caller specified skipping rows
     */
    @Default
    public long skipRows() {
        return 0;
    }

    /**
     * See {@link Builder#numRows}.
     * 
     * @return Whether the caller specified consuming a max number of rows.
     */
    @Default
    public long numRows() {
        return Long.MAX_VALUE;
    }

    /**
     * See {@link Builder#ignoreEmptyLines}.
     * 
     * @return Whether the caller specified ignoring empty lines.
     */
    @Default
    public boolean ignoreEmptyLines() {
        return false;
    }

    /**
     * See {@link Builder#allowMissingColumns}.
     * 
     * @return Whether the caller specified to allow missing columns
     */
    @Default
    public boolean allowMissingColumns() {
        return false;
    }

    /**
     * See {@link Builder#ignoreExcessColumns}.
     * 
     * @return Whether the caller specified to ignore excess columns.
     */
    @Default
    public boolean ignoreExcessColumns() {
        return false;
    }

    /**
     * See {@link Builder#hasHeaderRow}.
     * 
     * @return Whether the caller specified that the input has a header row.
     */
    @Default
    public boolean hasHeaderRow() {
        return true;
    }

    /**
     * See {@link Builder#skipHeaderRows}.
     * 
     * @return Whether the caller specified to skip some number of header rows.
     */
    @Default
    public long skipHeaderRows() {
        return 0;
    }

    private static final char defaultDelimiter = ',';

    /**
     * See {@link Builder#delimiter}.
     * 
     * @return The caller-specified delimiter.
     */
    @Default
    public char delimiter() {
        return defaultDelimiter;
    }


    private static final char defaultQuote = '"';

    /**
     * See {@link Builder#quote}.
     * 
     * @return The caller-specified quote character.
     */
    @Default
    public char quote() {
        return defaultQuote;
    }

    /**
     * See {@link Builder#ignoreSurroundingSpaces}.
     * 
     * @return Whether the caller specified to ignore surrounding spaces.
     */
    @Default
    public boolean ignoreSurroundingSpaces() {
        return true;
    }

    private static final boolean defaultTrim = false;

    /**
     * See {@link Builder#trim}.
     * 
     * @return Whether the caller specified to trim.
     */
    @Default
    public boolean trim() {
        return defaultTrim;
    }

    /**
     * See {@link Builder#concurrent}.
     * 
     * @return Whether the caller specified to run concurrently.
     */
    @Default
    public boolean concurrent() {
        return true;
    }

    private static final Duration defaultThreadShutdownTimeout = Duration.ofMinutes(1);

    /**
     * See {@link Builder#threadShutdownTimeout}.
     * 
     * @return The number of milliseconds the library should wait for the threads to shut down.
     */
    @Default
    public Duration threadShutdownTimeout() {
        return defaultThreadShutdownTimeout;
    }

    private static void check7BitAscii(String what, char c, List<String> problems) {
        if (c > 0x7f) {
            final String message = String.format("%s is set to '%c' but is required to be 7-bit ASCII",
                    what, c);
            problems.add(message);
        }
    }

    private static void checkNonnegative(String what, long value, List<String> problems) {
        if (value < 0) {
            final String message = String.format("%s is set to %d, but is required to be nonnegative",
                    what, value);
            problems.add(message);
        }
    }
}
