package io.deephaven.csv.tokenization;

import ch.randelshofer.fastdoubleparser.JavaDoubleParser;
import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.tokenization.Tokenizer.CustomDoubleParser;

/**
 * A {@link CustomDoubleParser} that uses a fast double parser. Java 11+ compatible.
 *
 * @see <a href="https://github.com/wrandelshofer/FastDoubleParser">FastDoubleParser</a>
 */
public final class FastCustomDoubleParser implements CustomDoubleParser {

    /**
     * Equivalent to {@code JavaDoubleParser.parseDouble(bs.data(), bs.begin(), bs.size())}.
     *
     * @param bs The byte slice.
     * @return The parsed value if successful. Otherwise, throws {@link NumberFormatException}.
     */
    @Override
    public double parse(ByteSlice bs) throws NumberFormatException {
        return JavaDoubleParser.parseDouble(bs.data(), bs.begin(), bs.size());
    }

    /**
     * Equivalent to {@link JavaDoubleParser#parseDouble}.
     *
     * @param cs the char sequence
     * @return The parsed value if successful. Otherwise, throws {@link NumberFormatException}.
     */
    @Override
    public double parse(CharSequence cs) throws NumberFormatException {
        return JavaDoubleParser.parseDouble(cs);
    }
}
