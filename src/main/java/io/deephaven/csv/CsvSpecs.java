package io.deephaven.csv;

import io.deephaven.csv.annotations.BuildableStyle;
import io.deephaven.csv.parsers.Parser;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.tokenization.JdkDoubleParser;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.tokenization.Tokenizer.CustomDoubleParser;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A specification object for parsing CSV input.
 */
@Immutable
@BuildableStyle
public abstract class CsvSpecs {
    public interface Builder {
        /**
         * Copy all of the parameters from {@code specs} into {@code this} builder.
         */
        Builder from(CsvSpecs specs);

        /**
         * Client-specified headers that can be used to override the existing headers in the input (if
         * {@link #hasHeaderRow()} is true), or to provide absent headers (if {@link #hasHeaderRow()} is false).
         */
        Builder headers(Iterable<String> elements);

        /**
         * Override a specific column header by number. This is applied after {@link #headers()}. Column numbers start
         * with 1.
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
         * @return the parsers
         */
        Builder parsers(Iterable<? extends Parser<?>> elements);

        /**
         * Used to force a specific parser for a specific column, specified by column name. Specifying a parser forgoes
         * column inference for that column.
         */
        Builder putParserForName(String columnName, Parser<?> parser);

        /**
         * Used to force a specific parser for a specific column, specified by column number. Column numbers start with
         * 1. Specifying a parser forgoes column inference for that column.
         */
        Builder putParserForIndex(int index, Parser<?> parser);

        /**
         * The default string that means "null value" in the input. This default is used for a column if there is no
         * corresponding {@link #nullValueLiteralForName()} or {@link #nullValueLiteralForName()} specified for that
         * column. Default value is "", the empty string. If the configured sink data structures do not support nulls,
         * the caller can set this to null so that nothing will be parsed as null.
         */
        Builder nullValueLiteral(@Nullable String nullValueLiteral);

        /**
         * The null value literal for specific columns, specified by column name. Specifying a null value literal for a
         * column overrides the value in {@link #nullValueLiteral()}.
         */
        Builder putNullValueLiteralForName(String columnName, String nullValueLiteral);

        /**
         * The null value literal for specific columns, specified by 1-based column index. Specifying a null value
         * literal for a column overrides the value in {@link #nullValueLiteral()}.
         */
        Builder putNullValueLiteralForIndex(int index, String nullValueLiteral);

        /**
         * The parser to uses when all values in the column are null. Defaults to {@code Parsers#STRING}.
         */
        Builder nullParser(Parser<?> parser);

        /**
         * The custom double parser. If not explicitly set, it will default to {@link CustomDoubleParser#load()} if
         * present, otherwise {@link JdkDoubleParser#INSTANCE}.
         */
        Builder customDoubleParser(Tokenizer.CustomDoubleParser customDoubleParser);

        /**
         * An optional low-level "timezone parser" that understands custom time zone strings. For example the Deephaven
         * system allows special timezones like " NY" and " MN" as in "2020-03-01T12:34:56 NY" (note also the explicit
         * space). The timezone parser must be reentrant.
         */
        Builder customTimeZoneParser(Tokenizer.CustomTimeZoneParser customTimeZoneParser);

        /**
         * An optional legalizer for column headers. The legalizer is a function that takes column names (as a
         * {@code String[]}) names and returns legal column names (as a {@code String[]}). The legalizer function is
         * permitted to reuse its input data structure. Defaults to {@code Function#identity()}.
         */
        Builder headerLegalizer(Function<String[], String[]> headerLegalizer);

        /**
         * An optional validator for column headers. The validator is a {@link Predicate} function that takes a column
         * name and returns a true if it is a legal column name, false otherwise. Defaults to {@code c -> true}.
         */
        Builder headerValidator(Predicate<String> headerValidator);

        /**
         * Number of data rows to skip before processing data. This is useful when you want to parse data in chunks.
         * Typically used together with {@link Builder#numRows}. Defaults to 0.
         */
        Builder skipRows(long skipRows);

        /**
         * Max number of rows to process. This is useful when you want to parse data in chunks. Typically used together
         * with {@link Builder#skipRows}. Defaults to {@link Long#MAX_VALUE}.
         */
        Builder numRows(long numRows);

        /**
         * Whether the library should skip over empty lines in the input. Defaults to false.
         */
        Builder ignoreEmptyLines(boolean ignoreEmptyLines);

        /**
         * Whether the library should allow missing columns in the input. If this flag is set, then rows that are too
         * short (that have fewer columns than the header row) will be interpreted as if the missing columns contained
         * the empty string. Defaults to false.
         */
        Builder allowMissingColumns(boolean allowMissingColumns);

        /**
         * Whether the library should allow excess columns in the input. If this flag is set, then rows that are too
         * long (that have more columns than the header row) will have those excess columns dropped. Defaults to false.
         */
        Builder ignoreExcessColumns(boolean ignoreExcessColumns);

        /**
         * Whether the input file has a header row. Defaults to true.
         */
        Builder hasHeaderRow(boolean hasHeaderRow);

        /**
         * The field delimiter character (the character that separates one column from the next). Must be 7-bit ASCII.
         * Defaults to {code ','}.
         */
        Builder delimiter(char delimiter);

        /**
         * The quote character (used when you want field or line delimiters to be interpreted as literal text. Must be
         * 7-bit ASCII. Defaults to {@code '"'}. For example:
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
         */
        Builder quote(char quote);

        /**
         * Whether to trim leading and trailing blanks from non-quoted values. Defaults to {@code true}.
         */
        Builder ignoreSurroundingSpaces(boolean ignoreSurroundingSpaces);

        /**
         * Whether to trim leading and trailing blanks from inside quoted values. Defaults to {@code false}.
         */
        Builder trim(boolean trim);

        /**
         * Whether to run concurrently. In particular, the operation that reads the raw file, breaks it into columns,
         * and stores that column text in memory can run in parallel with the column parsers, and the parsers can run in
         * parallel with each other.
         */
        Builder concurrent(boolean async);

        CsvSpecs build();
    }

    /**
     * Creates a builder for {@link CsvSpecs}.
     */
    public static Builder builder() {
        return ImmutableCsvSpecs.builder();
    }

    /**
     * A comma-separated-value delimited format.
     */
    public static CsvSpecs csv() {
        return builder().build();
    }

    /**
     * A tab-separated-value delimited format. Equivalent to {@code builder().delimiter('\t').build()}.
     */
    public static CsvSpecs tsv() {
        return builder().delimiter('\t').build();
    }

    /**
     * A header-less, CSV format. Equivalent to {@code builder().hasHeaderRow(false).build()}.
     */
    public static CsvSpecs headerless() {
        return builder().hasHeaderRow(false).build();
    }

    /**
     * See {@link Builder#headers}.
     */
    public abstract List<String> headers();

    /**
     * See {@link Builder#putHeaderForIndex}
     */
    public abstract Map<Integer, String> headerForIndex();

    /**
     * See {@link Builder#parsers}.
     */
    @Default
    public List<Parser<?>> parsers() {
        return Parsers.DEFAULT;
    }

    /**
     * See {@link Builder#putParserForName}.
     */
    public abstract Map<String, Parser<?>> parserForName();

    /**
     * See {@link Builder#putParserForIndex}.
     */
    public abstract Map<Integer, Parser<?>> parserForIndex();

    /**
     * See {@link Builder#nullValueLiteral}.
     */
    @Default
    @Nullable
    public String nullValueLiteral() {
        return "";
    }

    /**
     * See {@link Builder#nullValueLiteral}.
     */
    public abstract Map<String, String> nullValueLiteralForName();

    /**
     * See {@link Builder#putNullValueLiteralForIndex}.
     */
    public abstract Map<Integer, String> nullValueLiteralForIndex();

    /**
     * See {@link Builder#nullParser}.
     */
    @Default
    @Nullable
    public Parser<?> nullParser() {
        return Parsers.STRING;
    }

    /**
     * See {@link Builder#customDoubleParser}.
     */
    @Default
    public Tokenizer.CustomDoubleParser customDoubleParser() {
        return CustomDoubleParser.load().orElse(JdkDoubleParser.INSTANCE);
    }

    /**
     * See {@link Builder#customTimeZoneParser}.
     */
    @Default
    @Nullable
    public Tokenizer.CustomTimeZoneParser customTimeZoneParser() {
        return null;
    }

    /**
     * See {@link Builder#headerLegalizer}.
     */
    @Default
    public Function<String[], String[]> headerLegalizer() {
        return Function.identity();
    }

    /**
     * See {@link Builder#headerValidator}.
     */
    @Default
    public Predicate<String> headerValidator() {
        return c -> true;
    }

    /**
     * See {@link Builder#skipRows}.
     */
    @Default
    public long skipRows() {
        return 0;
    }

    /**
     * See {@link Builder#numRows}.
     */
    @Default
    public long numRows() {
        return Long.MAX_VALUE;
    }

    /**
     * See {@link Builder#ignoreEmptyLines}.
     */
    @Default
    public boolean ignoreEmptyLines() {
        return false;
    }

    /**
     * See {@link Builder#allowMissingColumns}.
     */
    @Default
    public boolean allowMissingColumns() {
        return false;
    }

    /**
     * See {@link Builder#ignoreExcessColumns}.
     */
    @Default
    public boolean ignoreExcessColumns() {
        return false;
    }

    /**
     * See {@link Builder#hasHeaderRow}.
     */
    @Default
    public boolean hasHeaderRow() {
        return true;
    }

    /**
     * See {@link Builder#delimiter}.
     */
    @Default
    public char delimiter() {
        return ',';
    }

    /**
     * See {@link Builder#quote}.
     */
    @Default
    public char quote() {
        return '"';
    }

    /**
     * See {@link Builder#ignoreSurroundingSpaces}.
     */
    @Default
    public boolean ignoreSurroundingSpaces() {
        return true;
    }

    /**
     * See {@link Builder#trim}.
     */
    @Default
    public boolean trim() {
        return false;
    }

    /**
     * See {@link Builder#concurrent}.
     */
    @Default
    public boolean concurrent() {
        return true;
    }
}
