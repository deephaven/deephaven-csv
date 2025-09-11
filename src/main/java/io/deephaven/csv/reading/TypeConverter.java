package io.deephaven.csv.reading;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.Source;
import java.lang.reflect.Array;

/**
 * A class used to copy data chunks of one numeric type to data chunks of another numeric type
 */
public class TypeConverter {
    private static final int BYTE = 1;
    private static final int SHORT = 2;
    private static final int INT = 3;
    private static final int LONG = 4;
    private static final int FLOAT = 5;
    private static final int DOUBLE = 6;

    /**
     * Copies a slice of data from the Source at the half-open interval [srcBegin,srcEnd) to the Sink, starting at
     * destBegin, using data chunks as intermediaries and doing value conversion. Basically the algorithm is: For each
     * chunk of data:
     * <ul>
     * <li>read a chunk into srcChunk from source</li>
     * <li>copy the data, doing value conversions, from srcChunk to destChunk</li>
     * <li>write a chunk of data from destChunk to dest</li>
     * </ul>
     *
     * @param source A Source holding the source data
     * @param dest A Sink holding the destination data
     * @param srcBegin The inclusive start position of the source data
     * @param srcEnd The exclusive end position of the source data
     * @param destBegin The inclusive start position of the destination data
     * @param srcChunk A temporary array of appropriate type to use to hold a chunk of source data
     * @param destChunk A temporary array of appropriate type to use to hold a chunk of dest data
     * @param isNullChunk A temporary array of bool to use to hold a chunk of "isNull" data
     * @param <TARRAY> The array type of the source data
     * @param <UARRAY> The array type of the destination data
     */
    public static <TARRAY, UARRAY> void copy(
            final Source<TARRAY> source,
            final Sink<UARRAY> dest,
            final long srcBegin,
            final long srcEnd,
            final long destBegin,
            final TARRAY srcChunk,
            final UARRAY destChunk,
            final boolean[] isNullChunk) {
        if (srcBegin == srcEnd) {
            return;
        }
        final int srcChunkSize = Array.getLength(srcChunk);
        final int destChunkSize = Array.getLength(destChunk);
        final int isNullChunkSize = isNullChunk.length;
        if (srcChunkSize != destChunkSize || srcChunkSize != isNullChunkSize) {
            final String message =
                    String.format(
                            "Logic error: chunk sizes differ: %d vs %d vs %d",
                            srcChunkSize, destChunkSize, isNullChunkSize);
            throw new RuntimeException(message);
        }

        final CopyOperation performCopy = getChunkCopierFor(srcChunk.getClass(), destChunk.getClass());

        long srcCurrent = srcBegin;
        long destCurrent = destBegin;
        while (srcCurrent != srcEnd) {
            final long srcEndToUse = Math.min(srcCurrent + srcChunkSize, srcEnd);
            final int copySize = Math.toIntExact(srcEndToUse - srcCurrent);
            final long destEndToUse = destCurrent + copySize;
            source.read(srcChunk, isNullChunk, srcCurrent, srcEndToUse);
            performCopy.accept(srcChunk, destChunk, copySize);
            dest.write(destChunk, isNullChunk, destCurrent, destEndToUse, false);
            srcCurrent = srcEndToUse;
            destCurrent = destEndToUse;
        }
    }

    private static <T, U> CopyOperation getChunkCopierFor(
            final Class<T> srcClass, final Class<U> destClass) {
        final int srcType = identify(srcClass);
        final int sinkType = identify(destClass);
        switch (srcType * 100 + sinkType) {
            case BYTE * 100 + SHORT:
                return TypeConverter::copyByteToShort;
            case BYTE * 100 + INT:
                return TypeConverter::copyByteToInt;
            case BYTE * 100 + LONG:
                return TypeConverter::copyByteToLong;
            case BYTE * 100 + FLOAT:
                return TypeConverter::copyByteToFloat;
            case BYTE * 100 + DOUBLE:
                return TypeConverter::copyByteToDouble;

            case SHORT * 100 + INT:
                return TypeConverter::copyShortToInt;
            case SHORT * 100 + LONG:
                return TypeConverter::copyShortToLong;
            case SHORT * 100 + FLOAT:
                return TypeConverter::copyShortToFloat;
            case SHORT * 100 + DOUBLE:
                return TypeConverter::copyShortToDouble;

            case INT * 100 + LONG:
                return TypeConverter::copyIntToLong;
            case INT * 100 + FLOAT:
                return TypeConverter::copyIntToFloat;
            case INT * 100 + DOUBLE:
                return TypeConverter::copyIntToDouble;

            case LONG * 100 + FLOAT:
                return TypeConverter::copyLongToFloat;
            case LONG * 100 + DOUBLE:
                return TypeConverter::copyLongToDouble;

            default: {
                final String message =
                        String.format(
                                "Logic error: don't have a converter from %s to %s",
                                srcClass.getCanonicalName(), destClass.getCanonicalName());
                throw new RuntimeException(message);
            }
        }
    }

    private static <T> int identify(final Class<T> chunkClass) {
        if (chunkClass == byte[].class) {
            return BYTE;
        }
        if (chunkClass == short[].class) {
            return SHORT;
        }
        if (chunkClass == int[].class) {
            return INT;
        }
        if (chunkClass == long[].class) {
            return LONG;
        }
        if (chunkClass == float[].class) {
            return FLOAT;
        }
        if (chunkClass == double[].class) {
            return DOUBLE;
        }
        throw new RuntimeException("Unsupported chunk type " + chunkClass.getCanonicalName());
    }

    private static void copyByteToShort(final Object src, final Object dest, final int size) {
        final byte[] srcTyped = (byte[]) src;
        final short[] destTyped = (short[]) dest;
        for (int ii = 0; ii < size; ++ii) {
            destTyped[ii] = srcTyped[ii];
        }
    }

    private static void copyByteToInt(final Object src, final Object dest, final int size) {
        final byte[] srcTyped = (byte[]) src;
        final int[] destTyped = (int[]) dest;
        for (int ii = 0; ii < size; ++ii) {
            destTyped[ii] = srcTyped[ii];
        }
    }

    private static void copyByteToLong(final Object src, final Object dest, final int size) {
        final byte[] srcTyped = (byte[]) src;
        final long[] destTyped = (long[]) dest;
        for (int ii = 0; ii < size; ++ii) {
            destTyped[ii] = srcTyped[ii];
        }
    }

    private static void copyByteToFloat(final Object src, final Object dest, final int size) {
        final byte[] srcTyped = (byte[]) src;
        final float[] destTyped = (float[]) dest;
        for (int ii = 0; ii < size; ++ii) {
            destTyped[ii] = srcTyped[ii];
        }
    }

    private static void copyByteToDouble(final Object src, final Object dest, final int size) {
        final byte[] srcTyped = (byte[]) src;
        final double[] destTyped = (double[]) dest;
        for (int ii = 0; ii < size; ++ii) {
            destTyped[ii] = srcTyped[ii];
        }
    }

    private static void copyShortToInt(final Object src, final Object dest, final int size) {
        final short[] srcTyped = (short[]) src;
        final int[] destTyped = (int[]) dest;
        for (int ii = 0; ii < size; ++ii) {
            destTyped[ii] = srcTyped[ii];
        }
    }

    private static void copyShortToLong(final Object src, final Object dest, final int size) {
        final short[] srcTyped = (short[]) src;
        final long[] destTyped = (long[]) dest;
        for (int ii = 0; ii < size; ++ii) {
            destTyped[ii] = srcTyped[ii];
        }
    }

    private static void copyShortToFloat(final Object src, final Object dest, final int size) {
        final short[] srcTyped = (short[]) src;
        final float[] destTyped = (float[]) dest;
        for (int ii = 0; ii < size; ++ii) {
            destTyped[ii] = srcTyped[ii];
        }
    }

    private static void copyShortToDouble(final Object src, final Object dest, final int size) {
        final short[] srcTyped = (short[]) src;
        final double[] destTyped = (double[]) dest;
        for (int ii = 0; ii < size; ++ii) {
            destTyped[ii] = srcTyped[ii];
        }
    }

    private static void copyIntToLong(final Object src, final Object dest, final int size) {
        final int[] srcTyped = (int[]) src;
        final long[] destTyped = (long[]) dest;
        for (int ii = 0; ii < size; ++ii) {
            destTyped[ii] = srcTyped[ii];
        }
    }

    private static void copyIntToFloat(final Object src, final Object dest, final int size) {
        final int[] srcTyped = (int[]) src;
        final float[] destTyped = (float[]) dest;
        for (int ii = 0; ii < size; ++ii) {
            destTyped[ii] = srcTyped[ii];
        }
    }

    private static void copyIntToDouble(final Object src, final Object dest, final int size) {
        final int[] srcTyped = (int[]) src;
        final double[] destTyped = (double[]) dest;
        for (int ii = 0; ii < size; ++ii) {
            destTyped[ii] = srcTyped[ii];
        }
    }

    private static void copyLongToFloat(final Object src, final Object dest, final int size) {
        final long[] srcTyped = (long[]) src;
        final float[] destTyped = (float[]) dest;
        for (int ii = 0; ii < size; ++ii) {
            destTyped[ii] = srcTyped[ii];
        }
    }

    private static void copyLongToDouble(final Object src, final Object dest, final int size) {
        final long[] srcTyped = (long[]) src;
        final double[] destTyped = (double[]) dest;
        for (int ii = 0; ii < size; ++ii) {
            destTyped[ii] = srcTyped[ii];
        }
    }

    private interface CopyOperation {
        void accept(Object src, Object dest, int size);
    }
}
