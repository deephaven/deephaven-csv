package io.deephaven.csv.parsers;

/** The parser for "microseconds since Unix epoch". */
public class TimestampMicrosParser extends TimestampParserBase {
    /**
     * Singleton instance.
     */
    public static final TimestampMicrosParser INSTANCE = new TimestampMicrosParser();

    private TimestampMicrosParser() {
        super(MICROSECOND_SCALE);
    }
}
