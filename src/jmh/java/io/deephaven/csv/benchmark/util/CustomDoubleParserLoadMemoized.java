package io.deephaven.csv.benchmark.util;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.tokenization.JdkDoubleParser;
import io.deephaven.csv.tokenization.Tokenizer.CustomDoubleParser;

public enum CustomDoubleParserLoadMemoized implements CustomDoubleParser {
    INSTANCE;

    public static double parseDouble(ByteSlice bs) {
        return INSTANCE.parse(bs);
    }

    public static double parseDouble(CharSequence cs) {
        return INSTANCE.parse(cs);
    }

    private final CustomDoubleParser parser;

    CustomDoubleParserLoadMemoized() {
        parser = CustomDoubleParser.load().orElse(JdkDoubleParser.INSTANCE);
    }

    @Override
    public double parse(ByteSlice bs) throws NumberFormatException {
        return parser.parse(bs);
    }

    @Override
    public double parse(CharSequence cs) throws NumberFormatException {
        return parser.parse(cs);
    }
}
