package io.deephaven.csv.sinks;

import io.deephaven.csv.util.MutableObject;

import java.lang.reflect.Array;
import java.util.function.IntFunction;

public class ArraySinkFactory implements SinkFactory {
    private final Byte byteSentinel;
    private final Short shortSentinel;
    private final Integer intSentinel;
    private final Long longSentinel;
    private final Float floatSentinel;
    private final Double doubleSentinel;
    private final Byte booleanAsByteSentinel;
    private final Character charSentinel;
    private final String stringSentinel;
    private final Long dateTimeAsLongSentinel;
    private final Long timestampAsLongSentinel;

    public ArraySinkFactory(Byte byteSentinel, Short shortSentinel, Integer intSentinel, Long longSentinel,
            Float floatSentinel, Double doubleSentinel,
            Byte booleanAsByteSentinel,
            Character charSentinel, String stringSentinel, Long dateTimeAsLongSentinel, Long timestampAsLongSentinel) {
        this.byteSentinel = byteSentinel;
        this.shortSentinel = shortSentinel;
        this.intSentinel = intSentinel;
        this.longSentinel = longSentinel;
        this.floatSentinel = floatSentinel;
        this.doubleSentinel = doubleSentinel;
        this.booleanAsByteSentinel = booleanAsByteSentinel;
        this.charSentinel = charSentinel;
        this.stringSentinel = stringSentinel;
        this.dateTimeAsLongSentinel = dateTimeAsLongSentinel;
        this.timestampAsLongSentinel = timestampAsLongSentinel;
    }

    @Override
    public Sink<byte[]> forByte(MutableObject<Source<byte[]>> source) {
        final ArrayByteSink result = new ArrayByteSink(byteSentinel);
        source.setValue(result);
        return result;
    }

    @Override
    public Byte reservedByte() {
        return byteSentinel;
    }

    @Override
    public Sink<short[]> forShort(MutableObject<Source<short[]>> source) {
        final ArrayShortSink result = new ArrayShortSink(shortSentinel);
        source.setValue(result);
        return result;
    }

    @Override
    public Short reservedShort() {
        return shortSentinel;
    }

    @Override
    public Sink<int[]> forInt(MutableObject<Source<int[]>> source) {
        final ArrayIntSink result = new ArrayIntSink(intSentinel);
        source.setValue(result);
        return result;
    }

    @Override
    public Integer reservedInt() {
        return intSentinel;
    }

    @Override
    public Sink<long[]> forLong(MutableObject<Source<long[]>> source) {
        final ArrayLongSink result = new ArrayLongSink(longSentinel);
        source.setValue(result);
        return result;
    }

    @Override
    public Long reservedLong() {
        return longSentinel;
    }

    @Override
    public Sink<float[]> forFloat() {
        return new ArrayFloatSink(floatSentinel);
    }

    @Override
    public Float reservedFloat() {
        return floatSentinel;
    }

    @Override
    public Sink<double[]> forDouble() {
        return new ArrayDoubleSink(doubleSentinel);
    }

    @Override
    public Double reservedDouble() {
        return doubleSentinel;
    }

    @Override
    public Sink<byte[]> forBooleanAsByte() {
        return new ArrayBooleanAsByteSink(booleanAsByteSentinel);
    }

    @Override
    public Sink<char[]> forChar() {
        return new ArrayCharSink(charSentinel);
    }

    @Override
    public Character reservedChar() {
        return charSentinel;
    }

    @Override
    public Sink<String[]> forString() {
        return new ArrayStringSink(stringSentinel);
    }

    @Override
    public String reservedString() {
        return stringSentinel;
    }

    @Override
    public Sink<long[]> forDateTimeAsLong() {
        return new ArrayDateTimeAsLongSink(dateTimeAsLongSentinel);
    }

    @Override
    public Long reservedDateTimeAsLong() {
        return dateTimeAsLongSentinel;
    }

    @Override
    public Sink<long[]> forTimestampAsLong() {
        return new ArrayTimestampAsLongSink(timestampAsLongSentinel);
    }

    @Override
    public Long reservedTimestampAsLong() {
        return timestampAsLongSentinel;
    }
}


abstract class ArraySinkBase<TARRAY> implements Sink<TARRAY> {
    private final int INITIAL_SIZE = 1024;

    private final IntFunction<TARRAY> arrayFactory;
    protected final boolean hasNullSentinel;
    protected TARRAY array;

    protected ArraySinkBase(final IntFunction<TARRAY> arrayFactory, final boolean hasNullSentinel) {
        this.arrayFactory = arrayFactory;
        this.hasNullSentinel = hasNullSentinel;
        this.array = arrayFactory.apply(INITIAL_SIZE);
    }

    @Override
    public final void write(
            final TARRAY src,
            final boolean[] isNull,
            final long destBegin,
            final long destEnd,
            boolean appending) {
        if (destBegin == destEnd) {
            return;
        }
        final int destBeginAsInt = Math.toIntExact(destBegin);
        final int destEndAsInt = Math.toIntExact(destEnd);
        final int destSize = Math.toIntExact(destEnd - destBegin);

        final int currentCapacity = Array.getLength(array);

        // Grow array if needed.
        if (currentCapacity < destEnd) {
            final int highBit = Integer.highestOneBit(destEndAsInt);
            final int newCapacity = destEndAsInt == highBit ? destEndAsInt : destEndAsInt * 2;
            final TARRAY newArray = arrayFactory.apply(newCapacity);
            System.arraycopy(array, 0, newArray, 0, currentCapacity);
            array = newArray;
        }

        // User-friendly null sentinel check
        if (hasNullSentinel) {
            // Fix null sentinels if necessary.
            nullFlagsToValues(isNull, src, destSize);
        } else {
            for (int i = 0; i < destSize; ++i) {
                if (isNull[i]) {
                    throw new RuntimeException(
                            "The input contains a null value, but sink is not configured with a null sentinel value");
                }
            }
        }

        // Write chunk to storage.
        System.arraycopy(src, 0, array, destBeginAsInt, destSize);
    }

    @Override
    public final Object getUnderlying() {
        return array;
    }

    protected abstract void nullFlagsToValues(
            final boolean[] isNull, final TARRAY values, final int size);
}


abstract class ArraySourceAndSinkBase<TARRAY> extends ArraySinkBase<TARRAY>
        implements io.deephaven.csv.sinks.Source<TARRAY>, Sink<TARRAY> {

    protected ArraySourceAndSinkBase(final IntFunction<TARRAY> arrayFactory, final boolean hasNullSentinel) {
        super(arrayFactory, hasNullSentinel);
    }

    @Override
    public void read(TARRAY dest, boolean[] isNull, long srcBegin, long srcEnd) {
        if (srcBegin == srcEnd) {
            return;
        }
        final int srcBeginAsInt = Math.toIntExact(srcBegin);
        final int srcSize = Math.toIntExact(srcEnd - srcBegin);

        // Copy data to dest.
        System.arraycopy(array, srcBeginAsInt, dest, 0, srcSize);
        if (hasNullSentinel) {
            nullValuesToFlags(dest, isNull, srcSize);
        }
    }

    protected abstract void nullValuesToFlags(
            final TARRAY values, final boolean[] isNull, final int size);
}


class ArrayByteSinkBase extends ArraySourceAndSinkBase<byte[]> {
    protected final Byte nullSentinel;

    public ArrayByteSinkBase(final Byte nullSentinel) {
        super(byte[]::new, nullSentinel != null);
        this.nullSentinel = nullSentinel;
    }

    @Override
    protected final void nullFlagsToValues(boolean[] isNull, byte[] values, int size) {
        for (int ii = 0; ii < size; ++ii) {
            if (isNull[ii]) {
                values[ii] = nullSentinel;
            }
        }
    }

    @Override
    protected final void nullValuesToFlags(byte[] values, boolean[] isNull, int size) {
        for (int ii = 0; ii < size; ++ii) {
            isNull[ii] = values[ii] == nullSentinel;
        }
    }
}


final class ArrayByteSink extends ArrayByteSinkBase {
    public ArrayByteSink(final Byte nullSentinel) {
        super(nullSentinel);
    }
}


final class ArrayShortSink extends ArraySourceAndSinkBase<short[]> {
    private final Short nullSentinel;

    public ArrayShortSink(final Short nullSentinel) {
        super(short[]::new, nullSentinel != null);
        this.nullSentinel = nullSentinel;
    }

    @Override
    protected void nullFlagsToValues(boolean[] isNull, short[] values, int size) {
        for (int ii = 0; ii < size; ++ii) {
            if (isNull[ii]) {
                values[ii] = nullSentinel;
            }
        }
    }

    @Override
    protected void nullValuesToFlags(short[] values, boolean[] isNull, int size) {
        for (int ii = 0; ii < size; ++ii) {
            isNull[ii] = values[ii] == nullSentinel;
        }
    }
}


final class ArrayIntSink extends ArraySourceAndSinkBase<int[]> {
    private final Integer nullSentinel;

    public ArrayIntSink(final Integer nullSentinel) {
        super(int[]::new, nullSentinel != null);
        this.nullSentinel = nullSentinel;
    }

    @Override
    protected void nullFlagsToValues(boolean[] isNull, int[] values, int size) {
        for (int ii = 0; ii < size; ++ii) {
            if (isNull[ii]) {
                values[ii] = nullSentinel;
            }
        }
    }

    @Override
    protected void nullValuesToFlags(int[] values, boolean[] isNull, int size) {
        for (int ii = 0; ii < size; ++ii) {
            isNull[ii] = values[ii] == nullSentinel;
        }
    }
}


class ArrayLongSinkBase extends ArraySourceAndSinkBase<long[]> {
    private final Long nullSentinel;

    public ArrayLongSinkBase(final Long nullSentinel) {
        super(long[]::new, nullSentinel != null);
        this.nullSentinel = nullSentinel;
    }

    @Override
    protected final void nullFlagsToValues(boolean[] isNull, long[] values, int size) {
        for (int ii = 0; ii < size; ++ii) {
            if (isNull[ii]) {
                values[ii] = nullSentinel;
            }
        }
    }

    @Override
    protected final void nullValuesToFlags(long[] values, boolean[] isNull, int size) {
        for (int ii = 0; ii < size; ++ii) {
            isNull[ii] = values[ii] == nullSentinel;
        }
    }
}


final class ArrayLongSink extends ArrayLongSinkBase {
    public ArrayLongSink(final Long nullSentinel) {
        super(nullSentinel);
    }
}


final class ArrayFloatSink extends ArraySinkBase<float[]> {
    private final Float nullSentinel;

    public ArrayFloatSink(final Float nullSentinel) {
        super(float[]::new, nullSentinel != null);
        this.nullSentinel = nullSentinel;
    }

    @Override
    protected void nullFlagsToValues(boolean[] isNull, float[] values, int size) {
        for (int ii = 0; ii < size; ++ii) {
            if (isNull[ii]) {
                values[ii] = nullSentinel;
            }
        }
    }
}


final class ArrayDoubleSink extends ArraySinkBase<double[]> {
    private final Double nullSentinel;

    public ArrayDoubleSink(final Double nullSentinel) {
        super(double[]::new, nullSentinel != null);
        this.nullSentinel = nullSentinel;
    }

    @Override
    protected void nullFlagsToValues(boolean[] isNull, double[] values, int size) {
        for (int ii = 0; ii < size; ++ii) {
            if (isNull[ii]) {
                values[ii] = nullSentinel;
            }
        }
    }
}


final class ArrayBooleanAsByteSink extends ArrayByteSinkBase {
    public ArrayBooleanAsByteSink(final Byte nullSentinel) {
        super(nullSentinel);
    }
}


final class ArrayCharSink extends ArraySinkBase<char[]> {
    private final Character nullSentinel;

    public ArrayCharSink(final Character nullSentinel) {
        super(char[]::new, nullSentinel != null);
        this.nullSentinel = nullSentinel;
    }

    @Override
    protected void nullFlagsToValues(boolean[] isNull, char[] values, int size) {
        for (int ii = 0; ii < size; ++ii) {
            if (isNull[ii]) {
                values[ii] = nullSentinel;
            }
        }
    }
}


final class ArrayStringSink extends ArraySinkBase<String[]> {
    private final String nullSentinel;

    public ArrayStringSink(final String nullSentinel) {
        // true because for Strings, "null" is a legitimate and reasonable "null sentinel".
        super(String[]::new, true);
        this.nullSentinel = nullSentinel;
    }

    @Override
    protected void nullFlagsToValues(boolean[] isNull, String[] values, int size) {
        for (int ii = 0; ii < size; ++ii) {
            if (isNull[ii]) {
                values[ii] = nullSentinel;
            }
        }
    }
}


final class ArrayDateTimeAsLongSink extends ArrayLongSinkBase {
    public ArrayDateTimeAsLongSink(final Long nullSentinel) {
        super(nullSentinel);
    }
}


final class ArrayTimestampAsLongSink extends ArrayLongSinkBase {
    public ArrayTimestampAsLongSink(final Long nullSentinel) {
        super(nullSentinel);
    }
}
