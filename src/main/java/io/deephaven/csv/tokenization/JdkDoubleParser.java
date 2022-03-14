package io.deephaven.csv.tokenization;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.tokenization.Tokenizer.CustomDoubleParser;

/**
 * A {@link CustomDoubleParser} that uses {@link Double#parseDouble(String)}.
 */
public enum JdkDoubleParser implements CustomDoubleParser {
    INSTANCE;

    /**
     * Equivalent to {@code Double.parseDouble(bs.toString())}.
     *
     * @param bs The byte slice.
     * @return the double
     */
    @Override
    public double parse(ByteSlice bs) throws NumberFormatException {
        return Double.parseDouble(bs.toString());
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
