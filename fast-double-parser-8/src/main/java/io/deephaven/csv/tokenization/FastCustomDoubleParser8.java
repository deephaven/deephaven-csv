package io.deephaven.csv.tokenization;

import ch.randelshofer.fastdoubleparser.FastDoubleParser;
import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.tokenization.Tokenizer.CustomDoubleParser;

/**
 * A {@link CustomDoubleParser} that uses a fast double parser. Java 8+ compatible.
 *
 * @see <a href="https://github.com/wrandelshofer/FastDoubleParser">FastDoubleParser</a>
 */
public final class FastCustomDoubleParser8 implements CustomDoubleParser {
    /**
     * Wraps the {@link ByteSlice} in a reusable {@link CharSequence} so that it can be passed to
     * {@link #parse(CharSequence)}
     *
     * @param bs The byte slice.
     * @return The parsed value if successful. Otherwise, throws NumberFormatException.
     */
    @Override
    public double parse(ByteSlice bs) throws NumberFormatException {
        return FastDoubleParser.parseDouble(bs);
    }

    /**
     * Equivalent to {@link FastDoubleParser#parseDouble}.
     *
     * @param cs the char sequence
     * @return The parsed value if successful. Otherwise, throws NumberFormatException.
     */
    @Override
    public double parse(CharSequence cs) throws NumberFormatException {
        return FastDoubleParser.parseDouble(cs);
    }
}
