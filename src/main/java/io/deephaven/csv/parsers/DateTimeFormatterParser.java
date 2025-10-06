package io.deephaven.csv.parsers;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.util.CsvReaderException;
import org.jetbrains.annotations.NotNull;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Objects;

import static java.time.temporal.ChronoField.INSTANT_SECONDS;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;

/**
 * A {@link DataType#CUSTOM} datetime parser.
 */
public final class DateTimeFormatterParser implements Parser<long[]> {

    /**
     * Creates a {@link DataType#CUSTOM} datetime parser according to the parsing logic of {@code formatter}. The
     * formatter must {@link DateTimeFormatter#parse(CharSequence) parse} into a {@link TemporalAccessor} that supports
     * fields {@link ChronoField#INSTANT_SECONDS} and {@link ChronoField#NANO_OF_SECOND}.
     *
     * @param formatter the formatter
     * @return the parser
     */
    public static DateTimeFormatterParser of(final DateTimeFormatter formatter) {
        if (!supportsInstant(formatter)) {
            throw new IllegalArgumentException();
        }
        return new DateTimeFormatterParser(formatter.withResolverFields(INSTANT_SECONDS, NANO_OF_SECOND));
    }

    private static boolean supportsInstant(final DateTimeFormatter formatter) {
        // TODO: we are really checking if the parsed result supports the fields
        // INSTANT_SECONDS and NANO_OF_SECOND; not sure if this check is equivalent.
        try {
            formatter.format(Instant.EPOCH);
            return true;
        } catch (final DateTimeException e) {
            return false;
        }
    }

    private final DateTimeFormatter formatter;

    private DateTimeFormatterParser(final DateTimeFormatter formatter) {
        this.formatter = Objects.requireNonNull(formatter);
    }

    @NotNull
    @Override
    public ParserContext<long[]> makeParserContext(
            final GlobalContext gctx,
            final int chunkSize) {
        final Sink<long[]> sink = gctx.sinkFactory().forDateTimeAsLong(gctx.colNum());
        return new ParserContext<>(sink, null, DataType.CUSTOM, new long[chunkSize]);
    }

    @Override
    public long tryParse(
            final GlobalContext gctx,
            final ParserContext<long[]> pctx,
            final IteratorHolder ih,
            final long begin,
            final long end,
            final boolean appending) throws CsvReaderException {
        final boolean[] nulls = gctx.nullChunk();

        final Sink<long[]> sink = pctx.sink();
        final Long reservedValue = gctx.sinkFactory().reservedDateTimeAsLong();
        final long[] values = pctx.valueChunk();

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
            final TemporalAccessor temporalAccessor;
            try {
                temporalAccessor = formatter.parse(ih.bs());
            } catch (final DateTimeParseException e) {
                break;
            }
            final long instantSecs;
            final int nanoOfSecond;
            try {
                instantSecs = temporalAccessor.getLong(INSTANT_SECONDS);
                nanoOfSecond = temporalAccessor.get(NANO_OF_SECOND);
            } catch (final DateTimeException | ArithmeticException e) {
                break;
            }
            // TODO: strict math?
            final long value = instantSecs * 1_000_000_000L + nanoOfSecond;
            if (reservedValue != null && value == reservedValue) {
                // If a reserved value is defined, it must not be present in the input.
                break;
            }
            if (ih.bs().size() > 1) {
                gctx.clearIsNullOrWidthOneSoFar();
            }
            values[chunkIndex] = value;
            nulls[chunkIndex] = false;
            ++chunkIndex;
        } while (ih.tryMoveNext());
        sink.write(values, nulls, current, current + chunkIndex, appending);
        return current + chunkIndex;
    }
}
