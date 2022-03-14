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
     * Equivalent to {@code FastDoubleParser.parseDouble(bs.toString())}.
     *
     * @param bs The byte slice.
     * @return the parsed double
     */
    @Override
    public double parse(ByteSlice bs) throws NumberFormatException {
        // Note: not as good of interface as ch.randelshofer:fastdoubleparser:0.3.0+
        return FastDoubleParser.parseDouble(bs.toString());
    }

    /**
     * Equivalent to {@code FastDoubleParser.parseDouble(cs)}.
     *
     * @param cs the char sequence
     * @return the parsed double
     */
    @Override
    public double parse(CharSequence cs) throws NumberFormatException {
        return FastDoubleParser.parseDouble(cs);
    }
}
