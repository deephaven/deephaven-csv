package io.deephaven.csv.parsers;

/** The parser for "nanoseconds since Unix epoch". */
public class TimestampNanosParser extends TimestampParserBase {
    /**
     * Singleton instance
     */
    public static final TimestampNanosParser INSTANCE = new TimestampNanosParser();

    private TimestampNanosParser() {
        super(NANOSECOND_SCALE);
    }
}
