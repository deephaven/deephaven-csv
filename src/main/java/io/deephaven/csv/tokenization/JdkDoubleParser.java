package io.deephaven.csv.tokenization;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.tokenization.Tokenizer.CustomDoubleParser;

import java.nio.charset.StandardCharsets;

/**
 * A {@link CustomDoubleParser} that uses {@link Double#parseDouble(String)}. Not actually an 'enum'. We use this as a
 * Java trick to get singletons.
 */
public enum JdkDoubleParser implements CustomDoubleParser {
    /**
     * Singleton instance.
     */
    INSTANCE;

    /**
     * Equivalent to {@code Double.parseDouble(bs.toString())}.
     *
     * @param bs The byte slice.
     * @return the double
     */
    @Override
    public double parse(ByteSlice bs) throws NumberFormatException {
        return Double.parseDouble(bs.toString(StandardCharsets.US_ASCII));
    }

    /**
     * Equivalent to {@code Double.parseDouble(cs.toString())}.
     *
     * @param cs the char sequence
     * @return the char sequence
     */
    @Override
    public double parse(CharSequence cs) throws NumberFormatException {
        return Double.parseDouble(cs.toString());
    }
}
