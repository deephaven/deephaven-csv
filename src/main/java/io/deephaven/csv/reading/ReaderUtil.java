package io.deephaven.csv.reading;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.tokenization.RangeTests;
import io.deephaven.csv.util.MutableInt;

public class ReaderUtil {
    public static String[] makeSyntheticHeaders(int numHeaders) {
        final String[] result = new String[numHeaders];
        for (int ii = 0; ii < result.length; ++ii) {
            result[ii] = "Column" + (ii + 1);
        }
        return result;
    }

    /**
     * Trim spaces and tabs from the front and back of the slice.
     *
     * @param cs The slice, modified in-place to have spaces and tabs (if any) removed.
     */
    public static void trimSpacesAndTabs(final ByteSlice cs) {
        final byte[] data = cs.data();
        int begin = cs.begin();
        int end = cs.end();
        while (begin != end && RangeTests.isSpaceOrTab(data[begin])) {
            ++begin;
        }
        while (begin != end && RangeTests.isSpaceOrTab(data[end - 1])) {
            --end;
        }
        cs.reset(data, begin, end);
    }

    /**
     * Get the expected length of a UTF-8 sequence, given its first byte, and its corresponding length in the specified
     * units (UTF-16 or UTF-32).
     * 
     * @param firstByte The first byte of the UTF-8 sequence.
     * @param numBytes The number of remaining bytes in the input field (including firstByte). If the UTF-8 sequence
     *        specifies a number of bytes larger than the number of remaining bytes, an exception is thrown.
     * @param useUtf32CountingConvention Whether 'charCountResult' should be in units of UTF-32 or UTF-16.
     * @param charCountResult The number of UTF-32 or UTF-16 units specified by the UTF-8 character.
     * @return The length of the UTF-8 sequence.
     */
    public static int getUtf8LengthAndCharLength(
            byte firstByte, int numBytes,
            boolean useUtf32CountingConvention, MutableInt charCountResult) {
        final int utf8Length = getUtf8Length(firstByte);
        if (utf8Length > numBytes) {
            throw new RuntimeException(String.format(
                    "The next UTF-8 character needs %d bytes but there are only %d left in the field",
                    utf8Length, numBytes));
        }
        final int numChars = useUtf32CountingConvention || utf8Length < 4 ? 1 : 2;
        charCountResult.setValue(numChars);
        return utf8Length;
    }

    /**
     * Calculate the expected length of a UTF-8 sequence, given its first byte.
     * 
     * @param firstByte The first byte of the sequence.
     * @return The length of the sequence, in the range 1..4 inclusive.
     */
    private static int getUtf8Length(byte firstByte) {
        if ((firstByte & 0x80) == 0) {
            // 0xxxxxxx
            // 1-byte UTF-8 character aka ASCII.
            // Last code point U+007F
            return 1;
        }
        if ((firstByte & 0xE0) == 0xC0) {
            // 110xxxxx
            // 2-byte UTF-8 character
            // Last code point U+07FF
            return 2;
        }
        if ((firstByte & 0xF0) == 0xE0) {
            // 1110xxxx
            // 3-byte UTF-8 character
            // Last code point U+FFFF
            return 3;
        }
        if ((firstByte & 0xF8) == 0xF0) {
            // 11110xxx
            // 4-byte UTF-8 character. Note: Java encodes all of these in two "char" variables.
            // Last code point U+10FFFF
            return 4;
        }
        throw new IllegalStateException(String.format("0x%x is not a valid starting byte for a UTF-8 sequence",
                firstByte));
    }
}
