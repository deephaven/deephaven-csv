package io.deephaven.csv.parsers;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.util.CsvReaderException;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;
import java.util.Objects;

/** The parser for the String type. */
public final class StringParser implements Parser<String[]> {
    /**
     * Singleton instance with {@link Charset#defaultCharset()}.
     */
    public static final StringParser INSTANCE = new StringParser(Charset.defaultCharset());

    /**
     * The String parser instance with {@code charset}.
     *
     * @param charset the charset
     * @return the String parser
     */
    public static StringParser of(final Charset charset) {
        return Charset.defaultCharset().equals(charset) ? INSTANCE : new StringParser(charset);
    }

    private final Charset charset;

    private StringParser(Charset charset) {
        this.charset = Objects.requireNonNull(charset);
    }

    @NotNull
    @Override
    public ParserContext<String[]> makeParserContext(final GlobalContext gctx, final int chunkSize) {
        final Sink<String[]> sink = gctx.sinkFactory().forString(gctx.colNum());
        return new ParserContext<>(sink, null, DataType.STRING, new String[chunkSize]);
    }

    @Override
    public long tryParse(
            final GlobalContext gctx,
            final ParserContext<String[]> pctx,
            IteratorHolder ih,
            final long begin,
            final long end,
            final boolean appending)
            throws CsvReaderException {
        final boolean[] nulls = gctx.nullChunk();

        final Sink<String[]> sink = pctx.sink();
        final String reservedValue = gctx.sinkFactory().reservedString();
        final String[] values = pctx.valueChunk();

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
            final String value = ih.bs().toString(charset);
            if (value.equals(reservedValue)) {
                // If a reserved value is defined, it must not be present in the input.
                break;
            }
            values[chunkIndex] = value;
            nulls[chunkIndex] = false;
            ++chunkIndex;
        } while (ih.tryMoveNext());
        sink.write(values, nulls, current, current + chunkIndex, appending);
        return current + chunkIndex;
    }
}
