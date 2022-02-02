package io.deephaven.csv;

import io.deephaven.csv.annotations.BuildableStyle;
import io.deephaven.csv.parsers.Parser;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.tokenization.Tokenizer;
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
        Builder from(CsvSpecs specs);

        Builder headers(Iterable<String> elements);

        Builder putHeaderForIndex(int index, String header);

        Builder parsers(Iterable<? extends Parser<?>> elements);

        Builder putParserForName(String columnName, Parser<?> parser);

        Builder putParserForIndex(int index, Parser<?> parser);

        Builder nullValueLiteral(String nullValueLiteral);

        Builder putNullValueLiteralForName(String columnName, String nullValueLiteral);

        Builder putNullValueLiteralForIndex(int index, String nullValueLiteral);

        Builder nullParser(Parser<?> parser);

        Builder hasHeaderRow(boolean hasHeaderRow);

        Builder delimiter(char delimiter);

        Builder quote(char quote);

        Builder ignoreSurroundingSpaces(boolean ignoreSurroundingSpaces);

        Builder trim(boolean trim);

        Builder concurrent(boolean async);

        Builder customTimeZoneParser(Tokenizer.CustomTimeZoneParser customTimeZoneParser);

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
     * Client-specified headers that can be used to override the existing headers in the input (if
     * {@link #hasHeaderRow()} is true), or to provide absent headers (if {@link #hasHeaderRow()} is false).
     */
    public abstract List<String> headers();

    /**
     * Override a specific column header by number. This is applied after {@link #headers()}. Column numbers start with
     * 1.
     */
    public abstract Map<Integer, String> headerForIndex();

    /**
     * Used to force a specific parser for a specific column, specified by column name. Specifying a parser forgoes
     * column inference for that column.
     */
    public abstract Map<String, Parser<?>> parserForName();

    /**
     * The parsers that the user wants to participate in type inference. Note that the order that the parsers in this
     * list matters only for custom parsers. In particular:
     * <ol>
     * <li>Standard system parsers (singletons from the {@link Parsers} class) will run in their standard precedence
     * order, regardless of the order they appear here.</li>
     * <li>All specified system parsers will be run before any specified custom parsers.</li>
     * <li>Custom parsers will be run in the order they are specified here.</li>
     * </ol>
     *
     * @return the parsers
     */
    @Default
    public List<Parser<?>> parsers() {
        return Parsers.DEFAULT;
    }

    /**
     * Used to force a specific parser for a specific column, specified by column number. Column numbers start with 1.
     * Specifying a parser forgoes column inference for that column.
     */
    public abstract Map<Integer, Parser<?>> parserForIndex();

    /**
     * The default string that means "null value" in the input. This default is used for a column if there is no
     * corresponding {@link #nullValueLiteralForName()} or {@link #nullValueLiteralForName()} specified for that column.
     */
    @Default
    public String nullValueLiteral() {
        return "";
    }

    /**
     * The null value literal for specific columns, specified by column name. Specifying a null value literal for a
     * column overrides the value in {@link #nullValueLiteral()}.
     */
    public abstract Map<String, String> nullValueLiteralForName();

    /**
     * The null value literal for specific columns, specified by 1-based column index. Specifying a null value literal
     * for a column overrides the value in {@link #nullValueLiteral()}.
     */
    public abstract Map<Integer, String> nullValueLiteralForIndex();

    /**
     * The parser to uses when all values in the column are null. Defaults to {@code Parsers#STRING}.
     */
    @Default
    @Nullable
    public Parser<?> nullParser() {
        return Parsers.STRING;
    }

    /**
     * An optional low-level parser that understands custom time zones.
     */
    @Default
    @Nullable
    public Tokenizer.CustomTimeZoneParser customTimeZoneParser() {
        return null;
    }

    /**
     * An optional legalizer for column headers. The legalizer is a function that takes column names (as a
     * {@code String[]}) names and returns legal column names (as a {@code String[]}). The legalizer function is
     * permitted to reuse its input data structure. Defaults to {@code Function#identity()}.
     */
    @Default
    public Function<String[], String[]> headerLegalizer() {
        return Function.identity();
    }

    /**
     * An optional validator for column headers. The validator is a {@link Predicate} function that takes a column name
     * and returns a true if it is a legal column name, false otherwise. Defaults to {@code c -> true}.
     */
    @Default
    public Predicate<String> headerValidator() {
        return c -> true;
    }

    /**
     * The header row flag. If {@code true}, the column names of the output table will be inferred from the first row of
     * the table. If {@code false}, the column names will be numbered numerically in the format "Column%d" with a
     * 1-based index. Defaults to {@code true}.
     */
    @Default
    public boolean hasHeaderRow() {
        return true;
    }

    /**
     * The field delimiter character (the character that separates one column from the next). Must be 7-bit ASCII.
     * Defaults to {code ','}.
     */
    @Default
    public char delimiter() {
        return ',';
    }

    /**
     * The quote character (used when you want field or line delimiters to be interpreted as literal text. Must be 7-bit
     * ASCII. Defaults to {@code '"'}. For example:
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
    @Default
    public char quote() {
        return '"';
    }

    /**
     * Whether to trim leading and trailing blanks from non-quoted values. Defaults to {@code true}.
     */
    @Default
    public boolean ignoreSurroundingSpaces() {
        return true;
    }

    /**
     * Whether to trim leading and trailing blanks from inside quoted values. Defaults to {@code false}.
     */
    @Default
    public boolean trim() {
        return false;
    }

    /**
     * Whether to run concurrently. In particular, the operation that reads the raw file, breaks it into columns, and
     * stores that column text in memory can run in parallel with the column parsers, and the parsers can run in
     * parallel with each other.
     */
    @Default
    public boolean concurrent() {
        return true;
    }
}
