package io.deephaven.csv.tokenization;

import ch.randelshofer.fastdoubleparser.FastDoubleParser;
import ch.randelshofer.fastdoubleparser.FastDoubleParserFromByteArray;
import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.tokenization.Tokenizer.CustomDoubleParser;

/**
 * A {@link CustomDoubleParser} that uses a fast double parser. Java 11+ compatible.
 *
 * @see <a href="https://github.com/wrandelshofer/FastDoubleParser">FastDoubleParser</a>
 */
public final class FastCustomDoubleParser implements CustomDoubleParser {

    /**
     * Equivalent to {@code FastDoubleParserFromByteArray.parseDouble(bs.data(), bs.begin(), bs.size())}.
     *
     * @param bs The byte slice.
     * @return the parsed double
     */
    @Override
    public double parse(ByteSlice bs) throws NumberFormatException {
        return FastDoubleParserFromByteArray.parseDouble(bs.data(), bs.begin(), bs.size());
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
