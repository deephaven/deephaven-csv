package io.deephaven.csv.sinks;

import io.deephaven.csv.util.MutableObject;

import java.util.function.IntFunction;

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
     * This factory method creates a {@link SinkFactory} from the corresponding lambdas. This version allows for more
     * peformant type inference, because it allows wider numeric parsers (like double) to directly read back data
     * written by narrower numeric parsers (like short) without having to reparse the ASCII text. Unfortunately this
     * requires the factory implementor to do a little more work, because they have to implement the Source&lt;T&gt;
     * interface for byte[], short[], int, and long[]. If the factory implementor does not want to do this work (or the
     * target data structure does not support reading back), the caller can invoke {@link SinkFactory#ofSimple} instead.
     *
     * As a service to the caller, we also make the provided {@link SinkFactory} threadsafe by synchronizing all the
     * forXXX methods. This is probably not necessary for most suppliers but we do it in order to provide an extra level
     * of protection.
     */
    static <TBYTESINK extends Sink<byte[]> & Source<byte[]>, TSHORTSINK extends Sink<short[]> & Source<short[]>, TINTSINK extends Sink<int[]> & Source<int[]>, TLONGSINK extends Sink<long[]> & Source<long[]>, TFLOATSINK extends Sink<float[]>, TDOUBLESINK extends Sink<double[]>, TBOOLASBYTESINK extends Sink<byte[]>, TCHARSINK extends Sink<char[]>, TSTRINGSINK extends Sink<String[]>, TDATETIMEASLONGSINK extends Sink<long[]>, TTIMESTAMPASLONGSINK extends Sink<long[]>> SinkFactory of(
            IntFunction<TBYTESINK> byteSinkSupplier,
            IntFunction<TSHORTSINK> shortSinkSupplier,
            IntFunction<TINTSINK> intSinkSupplier,
            IntFunction<TLONGSINK> longSinkSupplier,
            IntFunction<TFLOATSINK> floatSinkSupplier,
            IntFunction<TDOUBLESINK> doubleSinkSupplier,
            IntFunction<TBOOLASBYTESINK> booleanAsByteSinkSupplier,
            IntFunction<TCHARSINK> charSinkSupplier,
            IntFunction<TSTRINGSINK> stringSinkSupplier,
            IntFunction<TDATETIMEASLONGSINK> dateTimeAsLongSinkSupplier,
            IntFunction<TTIMESTAMPASLONGSINK> timestampAsLongSinkSupplier) {
        return new SinkFactoryImpl<>(byteSinkSupplier, null,
                shortSinkSupplier, null,
                intSinkSupplier, null,
                longSinkSupplier, null,
                floatSinkSupplier, null,
                doubleSinkSupplier, null,
                booleanAsByteSinkSupplier,
                charSinkSupplier, null,
                stringSinkSupplier, null,
                dateTimeAsLongSinkSupplier, null,
                timestampAsLongSinkSupplier, null);
    }

    /**
     * Variant of {@link SinkFactory#of} that allows you to specify reserved sentinel values that should be excluded
     * from their corresponding type.
     */
    static <TBYTESINK extends Sink<byte[]> & Source<byte[]>, TSHORTSINK extends Sink<short[]> & Source<short[]>, TINTSINK extends Sink<int[]> & Source<int[]>, TLONGSINK extends Sink<long[]> & Source<long[]>, TFLOATSINK extends Sink<float[]>, TDOUBLESINK extends Sink<double[]>, TBOOLASBYTESINK extends Sink<byte[]>, TCHARSINK extends Sink<char[]>, TSTRINGSINK extends Sink<String[]>, TDATETIMEASLONGSINK extends Sink<long[]>, TTIMESTAMPASLONGSINK extends Sink<long[]>> SinkFactory of(
            IntFunction<TBYTESINK> byteSinkSupplier,
            Byte reservedByte,
            IntFunction<TSHORTSINK> shortSinkSupplier,
            Short reservedShort,
            IntFunction<TINTSINK> intSinkSupplier,
            Integer reservedInt,
            IntFunction<TLONGSINK> longSinkSupplier,
            Long reservedLong,
            IntFunction<TFLOATSINK> floatSinkSupplier,
            Float reservedFloat,
            IntFunction<TDOUBLESINK> doubleSinkSupplier,
            Double reservedDouble,
            IntFunction<TBOOLASBYTESINK> booleanAsByteSinkSupplier,
            // no Byte reservedBooleanAsByte,
            IntFunction<TCHARSINK> charSinkSupplier,
            Character reservedChar,
            IntFunction<TSTRINGSINK> stringSinkSupplier,
            String reservedString,
            IntFunction<TDATETIMEASLONGSINK> dateTimeAsLongSinkSupplier,
            Long reservedDateTimeAsLong,
            IntFunction<TTIMESTAMPASLONGSINK> timestampAsLongSinkSupplier,
            Long reservedTimestampAsLong) {
        return new SinkFactoryImpl<>(byteSinkSupplier, reservedByte,
                shortSinkSupplier, reservedShort,
                intSinkSupplier, reservedInt,
                longSinkSupplier, reservedLong,
                floatSinkSupplier, reservedFloat,
                doubleSinkSupplier, reservedDouble,
                booleanAsByteSinkSupplier,
                charSinkSupplier, reservedChar,
                stringSinkSupplier, reservedString,
                dateTimeAsLongSinkSupplier, reservedDateTimeAsLong,
                timestampAsLongSinkSupplier, reservedTimestampAsLong);
    }

    /**
     * This factory method creates a {@link SinkFactory} from the corresponding lambdas. This version has somewhat less
     * peformant type inference, because it does not allow wider numeric parsers (like double) to directly read back
     * data written by narrower numeric parsers (like short). Instead the wider parser needs to reparse the ASCII text.
     * If the factory implementor prefers the more performant version, the caller can invoke {@link SinkFactory#of}
     * instead.
     *
     * As a service to the caller, we also make the provided {@link SinkFactory} threadsafe by synchronizing all the
     * forXXX methods. This is probably not necessary for most suppliers but we do it in order to provide an extra level
     * of protection.
     */
    static <TBYTESINK extends Sink<byte[]>, TSHORTSINK extends Sink<short[]>, TINTSINK extends Sink<int[]>, TLONGSINK extends Sink<long[]>, TFLOATSINK extends Sink<float[]>, TDOUBLESINK extends Sink<double[]>, TBOOLASBYTESINK extends Sink<byte[]>, TCHARSINK extends Sink<char[]>, TSTRINGSINK extends Sink<String[]>, TDATETIMEASLONGSINK extends Sink<long[]>, TTIMESTAMPASLONGSINK extends Sink<long[]>> SinkFactory ofSimple(
            IntFunction<TBYTESINK> byteSinkSupplier,
            IntFunction<TSHORTSINK> shortSinkSupplier,
            IntFunction<TINTSINK> intSinkSupplier,
            IntFunction<TLONGSINK> longSinkSupplier,
            IntFunction<TFLOATSINK> floatSinkSupplier,
            IntFunction<TDOUBLESINK> doubleSinkSupplier,
            IntFunction<TBOOLASBYTESINK> booleanAsByteSinkSupplier,
            IntFunction<TCHARSINK> charSinkSupplier,
            IntFunction<TSTRINGSINK> stringSinkSupplier,
            IntFunction<TDATETIMEASLONGSINK> dateTimeAsLongSinkSupplier,
            IntFunction<TTIMESTAMPASLONGSINK> timestampAsLongSinkSupplier) {
        return new SinkFactorySimpleImpl<>(byteSinkSupplier, null,
                shortSinkSupplier, null,
                intSinkSupplier, null,
                longSinkSupplier, null,
                floatSinkSupplier, null,
                doubleSinkSupplier, null,
                booleanAsByteSinkSupplier,
                charSinkSupplier, null,
                stringSinkSupplier, null,
                dateTimeAsLongSinkSupplier, null,
                timestampAsLongSinkSupplier, null);
    }

    /**
     * Variant of {@link SinkFactory#of} that allows you to specify reserved sentinel values that should be excluded
     * from their corresponding type.
     */
    static <TBYTESINK extends Sink<byte[]>, TSHORTSINK extends Sink<short[]>, TINTSINK extends Sink<int[]>, TLONGSINK extends Sink<long[]>, TFLOATSINK extends Sink<float[]>, TDOUBLESINK extends Sink<double[]>, TBOOLASBYTESINK extends Sink<byte[]>, TCHARSINK extends Sink<char[]>, TSTRINGSINK extends Sink<String[]>, TDATETIMEASLONGSINK extends Sink<long[]>, TTIMESTAMPASLONGSINK extends Sink<long[]>> SinkFactory ofSimple(
            IntFunction<TBYTESINK> byteSinkSupplier,
            Byte reservedByte,
            IntFunction<TSHORTSINK> shortSinkSupplier,
            Short reservedShort,
            IntFunction<TINTSINK> intSinkSupplier,
            Integer reservedInt,
            IntFunction<TLONGSINK> longSinkSupplier,
            Long reservedLong,
            IntFunction<TFLOATSINK> floatSinkSupplier,
            Float reservedFloat,
            IntFunction<TDOUBLESINK> doubleSinkSupplier,
            Double reservedDouble,
            IntFunction<TBOOLASBYTESINK> booleanAsByteSinkSupplier,
            // no Byte reservedBooleanAsByte,
            IntFunction<TCHARSINK> charSinkSupplier,
            Character reservedChar,
            IntFunction<TSTRINGSINK> stringSinkSupplier,
            String reservedString,
            IntFunction<TDATETIMEASLONGSINK> dateTimeAsLongSinkSupplier,
            Long reservedDateTimeAsLong,
            IntFunction<TTIMESTAMPASLONGSINK> timestampAsLongSinkSupplier,
            Long reservedTimestampAsLong) {
        return new SinkFactorySimpleImpl<>(byteSinkSupplier, reservedByte,
                shortSinkSupplier, reservedShort,
                intSinkSupplier, reservedInt,
                longSinkSupplier, reservedLong,
                floatSinkSupplier, reservedFloat,
                doubleSinkSupplier, reservedDouble,
                booleanAsByteSinkSupplier,
                charSinkSupplier, reservedChar,
                stringSinkSupplier, reservedString,
                dateTimeAsLongSinkSupplier, reservedDateTimeAsLong,
                timestampAsLongSinkSupplier, reservedTimestampAsLong);
    }

    /**
     * Factory method for simple array sinks. This is useful if you are coding up something quickly and just want a
     * sensible data structure.
     */
    static SinkFactory arrays() {
        return new ArraySinkFactory(null, null, null, null, null, null, null, null, null, null, null);
    }

    /**
     * Variant of {@link SinkFactory#arrays} that allows you to specify null sentinel values.
     */
    static SinkFactory arrays(Byte byteSentinel, Short shortSentinel,
            Integer intSentinel,
            Long longSentinel,
            Float floatSentinel,
            Double doubleSentinel,
            Byte booleanAsByteSentinel,
            Character charSentinel,
            String stringSentinel,
            Long dateTimeAsLongSentinel,
            Long timestampAsLongSentinel) {
        return new ArraySinkFactory(byteSentinel, shortSentinel,
                intSentinel,
                longSentinel,
                floatSentinel,
                doubleSentinel,
                booleanAsByteSentinel,
                charSentinel,
                stringSentinel,
                dateTimeAsLongSentinel,
                timestampAsLongSentinel);
    }


    /**
     * Provide a Sink and optional Source for the byte representation.
     * 
     * @param colNum The (zero-based) column number that this Sink will be used for.
     * @param source The optional Source that can be used to read back the data for faster type inference.
     **/
    Sink<byte[]> forByte(int colNum, MutableObject<Source<byte[]>> source);

    /** The optional reserved value for the byte representation. */
    Byte reservedByte();

    /**
     * Provide a Sink and optional Source for the short representation.
     * 
     * @param colNum The (zero-based) column number that this Sink will be used for.
     * @param source The optional Source that can be used to read back the data for faster type inference.
     **/
    Sink<short[]> forShort(int colNum, MutableObject<Source<short[]>> source);

    /** The optional reserved value for the short representation. */
    Short reservedShort();

    /**
     * Provide a Sink and optional Source for the int representation.
     * 
     * @param colNum The (zero-based) column number that this Sink will be used for.
     * @param source The optional Source that can be used to read back the data for faster type inference.
     **/
    Sink<int[]> forInt(int colNum, MutableObject<Source<int[]>> source);

    /** The optional reserved value for the int representation. */
    Integer reservedInt();

    /**
     * Provide a Sink and optional Source for the long representation.
     * 
     * @param colNum The (zero-based) column number that this Sink will be used for.
     * @param source The optional Source that can be used to read back the data for faster type inference.
     **/
    Sink<long[]> forLong(int colNum, MutableObject<Source<long[]>> source);

    /** The optional reserved value for the long representation. */
    Long reservedLong();

    /**
     * Provide a Sink for the float representation.
     * 
     * @param colNum The (zero-based) column number that this Sink will be used for.
     **/
    Sink<float[]> forFloat(int colNum);

    /** The optional reserved value for the float representation. */
    Float reservedFloat();

    /**
     * Provide a Sink for the double representation.
     * 
     * @param colNum The (zero-based) column number that this Sink will be used for.
     **/
    Sink<double[]> forDouble(int colNum);

    /** The optional reserved value for the double representation. */
    Double reservedDouble();

    /**
     * Provide a Sink for the booelan as byte representation.
     * 
     * @param colNum The (zero-based) column number that this Sink will be used for.
     **/
    Sink<byte[]> forBooleanAsByte(int colNum);

    // there is no reserved value for the boolean as byte representation, as none is needed.

    /**
     * Provide a Sink for the char representation.
     * 
     * @param colNum The (zero-based) column number that this Sink will be used for.
     **/
    Sink<char[]> forChar(int colNum);

    /** The optional reserved value for the char representation. */
    Character reservedChar();

    /**
     * Provide a Sink for the String representation.
     * 
     * @param colNum The (zero-based) column number that this Sink will be used for.
     **/
    Sink<String[]> forString(int colNum);

    /** The optional reserved value for the String representation. */
    String reservedString();

    /**
     * Provide a Sink for the datetime as long representation.
     * 
     * @param colNum The (zero-based) column number that this Sink will be used for.
     **/
    Sink<long[]> forDateTimeAsLong(int colNum);

    /** The optional reserved value for the DateTime (as long) representation. */
    Long reservedDateTimeAsLong();

    /**
     * Provide a Sink for the Timestamp (as long) representation.
     * 
     * @param colNum The (zero-based) column number that this Sink will be used for.
     **/
    Sink<long[]> forTimestampAsLong(int colNum);

    /** The optional reserved value for the Timestamp (as long) representation. */
    Long reservedTimestampAsLong();
}


abstract class SinkFactoryImplBase<TFLOATSINK extends Sink<float[]>, TDOUBLESINK extends Sink<double[]>, TBOOLASBYTESINK extends Sink<byte[]>, TCHARSINK extends Sink<char[]>, TSTRINGSINK extends Sink<String[]>, TDATETIMEASLONGSINK extends Sink<long[]>, TTIMESTAMPASLONGSINK extends Sink<long[]>>
        implements SinkFactory {
    private final Byte reservedByte;
    private final Short reservedShort;
    private final Integer reservedInt;
    private final Long reservedLong;
    private final IntFunction<TFLOATSINK> floatSinkSupplier;
    private final Float reservedFloat;
    private final IntFunction<TDOUBLESINK> doubleSinkSupplier;
    private final Double reservedDouble;
    private final IntFunction<TBOOLASBYTESINK> booleanAsByteSinkSupplier; // no Byte reservedBooleanAsByte,
    private final IntFunction<TCHARSINK> charSinkSupplier;
    private final Character reservedChar;
    private final IntFunction<TSTRINGSINK> stringSinkSupplier;
    private final String reservedString;
    private final IntFunction<TDATETIMEASLONGSINK> dateTimeAsLongSinkSupplier;
    private final Long reservedDateTimeAsLong;
    private final IntFunction<TTIMESTAMPASLONGSINK> timestampAsLongSinkSupplier;
    private final Long reservedTimestampAsLong;

    protected SinkFactoryImplBase(Byte reservedByte, Short reservedShort, Integer reservedInt, Long reservedLong,
            IntFunction<TFLOATSINK> floatSinkSupplier, Float reservedFloat,
            IntFunction<TDOUBLESINK> doubleSinkSupplier, Double reservedDouble,
            IntFunction<TBOOLASBYTESINK> booleanAsByteSinkSupplier,
            IntFunction<TCHARSINK> charSinkSupplier, Character reservedChar,
            IntFunction<TSTRINGSINK> stringSinkSupplier, String reservedString,
            IntFunction<TDATETIMEASLONGSINK> dateTimeAsLongSinkSupplier, Long reservedDateTimeAsLong,
            IntFunction<TTIMESTAMPASLONGSINK> timestampAsLongSinkSupplier, Long reservedTimestampAsLong) {
        this.reservedByte = reservedByte;
        this.reservedShort = reservedShort;
        this.reservedInt = reservedInt;
        this.reservedLong = reservedLong;
        this.floatSinkSupplier = floatSinkSupplier;
        this.reservedFloat = reservedFloat;
        this.doubleSinkSupplier = doubleSinkSupplier;
        this.reservedDouble = reservedDouble;
        this.booleanAsByteSinkSupplier = booleanAsByteSinkSupplier;
        this.charSinkSupplier = charSinkSupplier;
        this.reservedChar = reservedChar;
        this.stringSinkSupplier = stringSinkSupplier;
        this.reservedString = reservedString;
        this.dateTimeAsLongSinkSupplier = dateTimeAsLongSinkSupplier;
        this.reservedDateTimeAsLong = reservedDateTimeAsLong;
        this.timestampAsLongSinkSupplier = timestampAsLongSinkSupplier;
        this.reservedTimestampAsLong = reservedTimestampAsLong;
    }

    @Override
    public final Byte reservedByte() {
        return reservedByte;
    }

    @Override
    public final Short reservedShort() {
        return reservedShort;
    }

    @Override
    public final Integer reservedInt() {
        return reservedInt;
    }

    @Override
    public final Long reservedLong() {
        return reservedLong;
    }

    @Override
    public synchronized final Sink<float[]> forFloat(final int colNum) {
        return floatSinkSupplier.apply(colNum);
    }

    @Override
    public final Float reservedFloat() {
        return reservedFloat;
    }

    @Override
    public synchronized final Sink<double[]> forDouble(final int colNum) {
        return doubleSinkSupplier.apply(colNum);
    }

    @Override
    public final Double reservedDouble() {
        return reservedDouble;
    }

    @Override
    public synchronized final Sink<byte[]> forBooleanAsByte(final int colNum) {
        return booleanAsByteSinkSupplier.apply(colNum);
    }

    @Override
    public synchronized final Sink<char[]> forChar(final int colNum) {
        return charSinkSupplier.apply(colNum);
    }

    @Override
    public final Character reservedChar() {
        return reservedChar;
    }

    @Override
    public synchronized final Sink<String[]> forString(final int colNum) {
        return stringSinkSupplier.apply(colNum);
    }

    @Override
    public final String reservedString() {
        return reservedString;
    }

    @Override
    public synchronized final Sink<long[]> forDateTimeAsLong(final int colNum) {
        return dateTimeAsLongSinkSupplier.apply(colNum);
    }

    @Override
    public final Long reservedDateTimeAsLong() {
        return reservedDateTimeAsLong;
    }

    @Override
    public synchronized final Sink<long[]> forTimestampAsLong(final int colNum) {
        return timestampAsLongSinkSupplier.apply(colNum);
    }

    @Override
    public final Long reservedTimestampAsLong() {
        return reservedTimestampAsLong;
    }
}


final class SinkFactoryImpl<TBYTESINK extends Sink<byte[]> & Source<byte[]>, TSHORTSINK extends Sink<short[]> & Source<short[]>, TINTSINK extends Sink<int[]> & Source<int[]>, TLONGSINK extends Sink<long[]> & Source<long[]>, TFLOATSINK extends Sink<float[]>, TDOUBLESINK extends Sink<double[]>, TBOOLASBYTESINK extends Sink<byte[]>, TCHARSINK extends Sink<char[]>, TSTRINGSINK extends Sink<String[]>, TDATETIMEASLONGSINK extends Sink<long[]>, TTIMESTAMPASLONGSINK extends Sink<long[]>>
        extends
        SinkFactoryImplBase<TFLOATSINK, TDOUBLESINK, TBOOLASBYTESINK, TCHARSINK, TSTRINGSINK, TDATETIMEASLONGSINK, TTIMESTAMPASLONGSINK> {
    private final IntFunction<TBYTESINK> byteSinkSupplier;
    private final IntFunction<TSHORTSINK> shortSinkSupplier;
    private final IntFunction<TINTSINK> intSinkSupplier;
    private final IntFunction<TLONGSINK> longSinkSupplier;

    public SinkFactoryImpl(
            IntFunction<TBYTESINK> byteSinkSupplier, Byte reservedByte,
            IntFunction<TSHORTSINK> shortSinkSupplier, Short reservedShort,
            IntFunction<TINTSINK> intSinkSupplier, Integer reservedInt,
            IntFunction<TLONGSINK> longSinkSupplier, Long reservedLong,
            IntFunction<TFLOATSINK> floatSinkSupplier, Float reservedFloat,
            IntFunction<TDOUBLESINK> doubleSinkSupplier, Double reservedDouble,
            IntFunction<TBOOLASBYTESINK> booleanAsByteSinkSupplier,
            IntFunction<TCHARSINK> charSinkSupplier, Character reservedChar,
            IntFunction<TSTRINGSINK> stringSinkSupplier, String reservedString,
            IntFunction<TDATETIMEASLONGSINK> dateTimeAsLongSinkSupplier, Long reservedDateTimeAsLong,
            IntFunction<TTIMESTAMPASLONGSINK> timestampAsLongSinkSupplier, Long reservedTimestampAsLong) {
        super(reservedByte, reservedShort, reservedInt, reservedLong, floatSinkSupplier, reservedFloat,
                doubleSinkSupplier, reservedDouble, booleanAsByteSinkSupplier, charSinkSupplier, reservedChar,
                stringSinkSupplier, reservedString, dateTimeAsLongSinkSupplier, reservedDateTimeAsLong,
                timestampAsLongSinkSupplier, reservedTimestampAsLong);
        this.byteSinkSupplier = byteSinkSupplier;
        this.shortSinkSupplier = shortSinkSupplier;
        this.intSinkSupplier = intSinkSupplier;
        this.longSinkSupplier = longSinkSupplier;
    }

    @Override
    public synchronized Sink<byte[]> forByte(final int colNum, final MutableObject<Source<byte[]>> source) {
        final TBYTESINK result = byteSinkSupplier.apply(colNum);
        source.setValue(result);
        return result;
    }

    @Override
    public synchronized Sink<short[]> forShort(final int colNum, MutableObject<Source<short[]>> source) {
        final TSHORTSINK result = shortSinkSupplier.apply(colNum);
        source.setValue(result);
        return result;
    }

    @Override
    public synchronized Sink<int[]> forInt(final int colNum, MutableObject<Source<int[]>> source) {
        final TINTSINK result = intSinkSupplier.apply(colNum);
        source.setValue(result);
        return result;
    }

    @Override
    public synchronized Sink<long[]> forLong(final int colNum, MutableObject<Source<long[]>> source) {
        final TLONGSINK result = longSinkSupplier.apply(colNum);
        source.setValue(result);
        return result;
    }
}


final class SinkFactorySimpleImpl<TBYTESINK extends Sink<byte[]>, TSHORTSINK extends Sink<short[]>, TINTSINK extends Sink<int[]>, TLONGSINK extends Sink<long[]>, TFLOATSINK extends Sink<float[]>, TDOUBLESINK extends Sink<double[]>, TBOOLASBYTESINK extends Sink<byte[]>, TCHARSINK extends Sink<char[]>, TSTRINGSINK extends Sink<String[]>, TDATETIMEASLONGSINK extends Sink<long[]>, TTIMESTAMPASLONGSINK extends Sink<long[]>>
        extends
        SinkFactoryImplBase<TFLOATSINK, TDOUBLESINK, TBOOLASBYTESINK, TCHARSINK, TSTRINGSINK, TDATETIMEASLONGSINK, TTIMESTAMPASLONGSINK> {
    private final IntFunction<TBYTESINK> byteSinkSupplier;
    private final IntFunction<TSHORTSINK> shortSinkSupplier;
    private final IntFunction<TINTSINK> intSinkSupplier;
    private final IntFunction<TLONGSINK> longSinkSupplier;

    public SinkFactorySimpleImpl(
            IntFunction<TBYTESINK> byteSinkSupplier, Byte reservedByte,
            IntFunction<TSHORTSINK> shortSinkSupplier, Short reservedShort,
            IntFunction<TINTSINK> intSinkSupplier, Integer reservedInt,
            IntFunction<TLONGSINK> longSinkSupplier, Long reservedLong,
            IntFunction<TFLOATSINK> floatSinkSupplier, Float reservedFloat,
            IntFunction<TDOUBLESINK> doubleSinkSupplier, Double reservedDouble,
            IntFunction<TBOOLASBYTESINK> booleanAsByteSinkSupplier,
            IntFunction<TCHARSINK> charSinkSupplier, Character reservedChar,
            IntFunction<TSTRINGSINK> stringSinkSupplier, String reservedString,
            IntFunction<TDATETIMEASLONGSINK> dateTimeAsLongSinkSupplier, Long reservedDateTimeAsLong,
            IntFunction<TTIMESTAMPASLONGSINK> timestampAsLongSinkSupplier, Long reservedTimestampAsLong) {
        super(reservedByte, reservedShort, reservedInt, reservedLong, floatSinkSupplier, reservedFloat,
                doubleSinkSupplier, reservedDouble, booleanAsByteSinkSupplier, charSinkSupplier, reservedChar,
                stringSinkSupplier, reservedString, dateTimeAsLongSinkSupplier, reservedDateTimeAsLong,
                timestampAsLongSinkSupplier, reservedTimestampAsLong);
        this.byteSinkSupplier = byteSinkSupplier;
        this.shortSinkSupplier = shortSinkSupplier;
        this.intSinkSupplier = intSinkSupplier;
        this.longSinkSupplier = longSinkSupplier;
    }

    @Override
    public synchronized Sink<byte[]> forByte(final int colNum, final MutableObject<Source<byte[]>> source) {
        source.setValue(null);
        return byteSinkSupplier.apply(colNum);
    }

    @Override
    public synchronized Sink<short[]> forShort(final int colNum, MutableObject<Source<short[]>> source) {
        source.setValue(null);
        return shortSinkSupplier.apply(colNum);
    }

    @Override
    public synchronized Sink<int[]> forInt(final int colNum, MutableObject<Source<int[]>> source) {
        source.setValue(null);
        return intSinkSupplier.apply(colNum);
    }

    @Override
    public synchronized Sink<long[]> forLong(final int colNum, MutableObject<Source<long[]>> source) {
        source.setValue(null);
        return longSinkSupplier.apply(colNum);
    }
}
