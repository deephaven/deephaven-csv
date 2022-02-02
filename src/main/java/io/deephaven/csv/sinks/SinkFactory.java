package io.deephaven.csv.sinks;

import io.deephaven.csv.util.MutableObject;

import java.util.function.Supplier;

/**
 * An interface which allows the CsvReader to write to column data structures whose details it is unaware of. Using this
 * interface, the caller provides factory methods that make a Sink&lt;TARRAY&gt; for the corresponding data type. The
 * integral parsers (byte, short, int, long) also provide a Source&lt;TARRAY&gt; via an out parameter, because the
 * inference algorithm wants a fast path for reading back data it has already written. This is used in the case where
 * the algorithm makes some forward progress on a numeric type but then decides to fall back to a wider numeric type.
 * The system also supports more general kinds of fallback (e.g. from int to string), but in cases like that the system
 * just reparses the original input text rather than asking the collection to read the data back.
 *
 * <p>
 * For example, if the system has parsed N shorts for a given column and then encounters an int value that doesn't fit
 * in a short (or, alternatively, it encounters a reserved short and needs to reject it), it will read back the shorts
 * already written and write them to an integer sink instead.
 *
 * <p>
 * The methods allow the caller to specify "reserved" values for types where it makes sense to have one. If a reserved
 * value is encountered, the type inference process will move to the next wider type and try again. In typical practice
 * this is used in systems that have a reserved sentinel value that represents null. For example, for a byte column, a
 * system might reserve the value ((byte)-128) to represent the null byte, yet allow ((short)-128) to be a permissible
 * short value. Likewise a system might reserve the value ((short)-32768) to represent the null short, but allow
 * ((int)-32768) to be a permissible int value.
 *
 * <p>
 * Thread safety: Implementing classes are required to be threadsafe, because the methods in this interface are likely
 * to be invoked concurrently.
 */
public interface SinkFactory {
    /**
     * Create a {@link SinkFactory} from the corresponding lambdas. As a service to the caller, we make the
     * {@link SinkFactory} threadsafe by synchronizing all the forXXX methods. This is probably not necessary for most
     * suppliers but we do it in order to provide an extra level of protection.
     */
    static <TBYTESINK extends Sink<byte[]> & Source<byte[]>, TSHORTSINK extends Sink<short[]> & Source<short[]>, TINTSINK extends Sink<int[]> & Source<int[]>, TLONGSINK extends Sink<long[]> & Source<long[]>, TFLOATSINK extends Sink<float[]>, TDOUBLESINK extends Sink<double[]>, TBOOLASBYTESINK extends Sink<byte[]>, TCHARSINK extends Sink<char[]>, TSTRINGSINK extends Sink<String[]>, TDATETIMEASLONGSINK extends Sink<long[]>, TTIMESTAMPASLONGSINK extends Sink<long[]>> SinkFactory of(
            Supplier<TBYTESINK> byteSinkSupplier,
            Byte reservedByte,
            Supplier<TSHORTSINK> shortSinkSupplier,
            Short reservedShort,
            Supplier<TINTSINK> intSinkSupplier,
            Integer reservedInt,
            Supplier<TLONGSINK> longSinkSupplier,
            Long reservedLong,
            Supplier<TFLOATSINK> floatSinkSupplier,
            Float reservedFloat,
            Supplier<TDOUBLESINK> doubleSinkSupplier,
            Double reservedDouble,
            Supplier<TBOOLASBYTESINK> booleanAsByteSinkSupplier, // no Byte reservedBooleanAsByte,
            Supplier<TCHARSINK> charSinkSupplier,
            Character reservedChar,
            Supplier<TSTRINGSINK> stringSinkSupplier,
            String reservedString,
            Supplier<TDATETIMEASLONGSINK> dateTimeAsLongSinkSupplier,
            Long reservedDateTimeAsLong,
            Supplier<TTIMESTAMPASLONGSINK> timestampAsLongSinkSupplier,
            Long reservedTimestampAsLong) {
        return new SinkFactory() {
            @Override
            public synchronized Sink<byte[]> forByte(MutableObject<Source<byte[]>> source) {
                final TBYTESINK result = byteSinkSupplier.get();
                source.setValue(result);
                return result;
            }

            @Override
            public Byte reservedByte() {
                return reservedByte;
            }

            @Override
            public synchronized Sink<short[]> forShort(MutableObject<Source<short[]>> source) {
                final TSHORTSINK result = shortSinkSupplier.get();
                source.setValue(result);
                return result;
            }

            @Override
            public Short reservedShort() {
                return reservedShort;
            }

            @Override
            public synchronized Sink<int[]> forInt(MutableObject<Source<int[]>> source) {
                final TINTSINK result = intSinkSupplier.get();
                source.setValue(result);
                return result;
            }

            @Override
            public Integer reservedInt() {
                return reservedInt;
            }

            @Override
            public synchronized Sink<long[]> forLong(MutableObject<Source<long[]>> source) {
                final TLONGSINK result = longSinkSupplier.get();
                source.setValue(result);
                return result;
            }

            @Override
            public Long reservedLong() {
                return reservedLong;
            }

            @Override
            public synchronized Sink<float[]> forFloat() {
                return floatSinkSupplier.get();
            }

            @Override
            public Float reservedFloat() {
                return reservedFloat;
            }

            @Override
            public synchronized Sink<double[]> forDouble() {
                return doubleSinkSupplier.get();
            }

            @Override
            public Double reservedDouble() {
                return reservedDouble;
            }

            @Override
            public synchronized Sink<byte[]> forBooleanAsByte() {
                return booleanAsByteSinkSupplier.get();
            }

            @Override
            public synchronized Sink<char[]> forChar() {
                return charSinkSupplier.get();
            }

            @Override
            public Character reservedChar() {
                return reservedChar;
            }

            @Override
            public synchronized Sink<String[]> forString() {
                return stringSinkSupplier.get();
            }

            @Override
            public String reservedString() {
                return reservedString;
            }

            @Override
            public synchronized Sink<long[]> forDateTimeAsLong() {
                return dateTimeAsLongSinkSupplier.get();
            }

            @Override
            public Long reservedDateTimeAsLong() {
                return reservedDateTimeAsLong;
            }

            @Override
            public synchronized Sink<long[]> forTimestampAsLong() {
                return timestampAsLongSinkSupplier.get();
            }

            @Override
            public Long reservedTimestampAsLong() {
                return reservedTimestampAsLong;
            }
        };
    }

    /** Provide a Sink and a Source for the byte representation. */
    Sink<byte[]> forByte(MutableObject<Source<byte[]>> source);

    /** The optional reserved value for the byte representation. */
    Byte reservedByte();

    /** Provide a Sink and a Source for the short representation. */
    Sink<short[]> forShort(MutableObject<Source<short[]>> source);

    /** The optional reserved value for the short representation. */
    Short reservedShort();

    /** Provide a Sink and a Source for the int representation. */
    Sink<int[]> forInt(MutableObject<Source<int[]>> source);

    /** The optional reserved value for the int representation. */
    Integer reservedInt();

    /** Provide a Sink and a Source for the long representation. */
    Sink<long[]> forLong(MutableObject<Source<long[]>> source);

    /** The optional reserved value for the long representation. */
    Long reservedLong();

    /** Provide a Sink for the float representation. */
    Sink<float[]> forFloat();

    /** The optional reserved value for the float representation. */
    Float reservedFloat();

    /** Provide a Sink for the double representation. */
    Sink<double[]> forDouble();

    /** The optional reserved value for the double representation. */
    Double reservedDouble();

    /** Provide a Sink for the boolean (as byte) representation. */
    Sink<byte[]> forBooleanAsByte();

    // there is no reserved value for the boolean as byte representation, as none is needed.

    /** Provide a Sink for the char representation. */
    Sink<char[]> forChar();

    /** The optional reserved value for the char representation. */
    Character reservedChar();

    /** Provide a Sink for the String representation. */
    Sink<String[]> forString();

    /** The optional reserved value for the String representation. */
    String reservedString();

    /** Provide a Sink for the DateTime (as long) representation. */
    Sink<long[]> forDateTimeAsLong();

    /** The optional reserved value for the DateTime (as long) representation. */
    Long reservedDateTimeAsLong();

    /** Provide a Sink for the Timestamp (as long) representation. */
    Sink<long[]> forTimestampAsLong();

    /** The optional reserved value for the Timestamp (as long) representation. */
    Long reservedTimestampAsLong();
}
