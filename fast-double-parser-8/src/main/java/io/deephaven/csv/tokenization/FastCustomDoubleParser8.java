package io.deephaven.csv.tokenization;

import ch.randelshofer.fastdoubleparser.JavaDoubleParser;
import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.tokenization.Tokenizer.CustomDoubleParser;

/**
 * A {@link CustomDoubleParser} that uses a fast double parser. Java 8+ compatible.
 *
 * @see <a href="https://github.com/wrandelshofer/FastDoubleParser">FastDoubleParser</a>
 */
public final class FastCustomDoubleParser8 implements CustomDoubleParser {
    /**
     * Equivalent to {@link JavaDoubleParser#parseDouble} (with the {@link ByteSlice} interpreted as a
     * {@link CharSequence}.
     *
     * @param bs The byte slice.
     * @return The parsed value if successful. Otherwise, throws {@link NumberFormatException}.
     */
    @Override
    public double parse(ByteSlice bs) throws NumberFormatException {
        // FastDoubleParser used to throw NumberFormatException on empty input. Now it throws IllegalArgumentException.
        // We will make a change request at the FastDoubleParser project.
        if (bs.size() == 0) {
            throw new NumberFormatException("empty input");
        }
        return JavaDoubleParser.parseDouble(bs);
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
