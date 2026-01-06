package io.deephaven.csv.testutil;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.parsers.DataType;
import io.deephaven.csv.parsers.IteratorHolder;
import io.deephaven.csv.parsers.Parser;
import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.tokenization.RangeTests;
import io.deephaven.csv.util.CsvReaderException;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;

public class MyBigDecimalParser implements Parser<BigDecimal[]> {
    @NotNull
    @Override
    public ParserContext<BigDecimal[]> makeParserContext(GlobalContext gctx, int chunkSize) {
        final MyReferenceTypeSink sink = new MyReferenceTypeSink();
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
