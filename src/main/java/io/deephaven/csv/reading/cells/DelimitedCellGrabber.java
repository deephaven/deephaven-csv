package io.deephaven.csv.reading.cells;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.containers.GrowableByteBuffer;
import io.deephaven.csv.reading.ReaderUtil;
import io.deephaven.csv.tokenization.RangeTests;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.csv.util.MutableBoolean;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class is used to traverse over text from a Reader, understanding both field and line delimiters, as well as the
 * CSV quoting convention, and breaking the text into cells for use by the calling code.
 */
public final class DelimitedCellGrabber implements CellGrabber {
    /** Size of chunks to read from the {@link InputStream}. */
    public static final int BUFFER_SIZE = 65536;
    /** The {@link InputStream} for the input. */
    private final InputStream inputStream;
    /** The configured CSV quote character (typically '"'). Must be 7-bit ASCII. */
    private final byte quoteChar;
    /**
     * The configured CVS escape character. Must be 7-bit ASCII. If configured to null in CsvSpecs, we set it to the
     * illegal UTF-8 byte 0xff so it has no effect.
     */
    private final byte escapeChar;
    /** The configured CVS field delimiter (typically ','). Must be 7-bit ASCII. */
    private final byte fieldDelimiter;
    /** Whether to trim leading and trailing blanks from non-quoted values. */
    private final boolean ignoreSurroundingSpaces;
    /** Whether to trim leading and trailing blanks from inside quoted values. */
    private final boolean trim;
    /** The current chunk we have read from the file. */
    private final byte[] buffer;
    /** Size of the last buffer chunk read. */
    private int size;
    /** Current offset in the buffer chunk. */
    private int offset;
    /** Starting offset of a contiguous span of characters we are scanning from the buffer chunk. */
    private int startOffset;
    /**
     * A side buffer we have to use for edge cases. Normally we try to return a {@link ByteSlice} which shares our
     * buffer[] array. But we can't do that when the input cell spans more than one buffer[] chunk, or when the input
     * cell does not exactly represent the output. This latter case can happen for example when an escaped quote ("")
     * needs to be returned as a single quotation mark ("). So if our input is hello""there, then we can't directly
     * return a slice of the input array, because actually we need hello"there (one quotation mark, not two). Another
     * case where this can happen is when the escape character is enabled and we encounter an escape like \, or \n.
     */
    private final GrowableByteBuffer spillBuffer;
    /**
     * Zero-based row number of the input stream. This is for informational purposes only and in particular does NOT
     * refer to the number of data rows in the input. (This is because the data rows may be split across multiple lines
     * and because there may or may not be headers). We track this number for the benefit of the caller, who may want to
     * issue an informative error message when there is a problem.
     */
    private int physicalRowNum;

    /**
     * Constructor.
     * 
     * @param inputStream The input, represented as UTF-8 bytes.
     * @param quoteChar The configured quote char. Typically "
     * @param escapeChar The configured escape char. Defaults to our representation of 'none' but if the feature is
     *        desired, is typically set to \
     * @param fieldDelimiter The configured field delimiter. Typically ,
     * @param ignoreSurroundingSpaces Whether to ignore surrounding spaces
     * @param trim Whether to trim spaces inside quoted values.
     */
    public DelimitedCellGrabber(
            final InputStream inputStream,
            final byte quoteChar,
            final byte escapeChar,
            final byte fieldDelimiter,
            final boolean ignoreSurroundingSpaces,
            final boolean trim) {
        this.inputStream = inputStream;
        this.quoteChar = quoteChar;
        this.escapeChar = escapeChar;
        this.fieldDelimiter = fieldDelimiter;
        this.ignoreSurroundingSpaces = ignoreSurroundingSpaces;
        this.trim = trim;
        this.buffer = new byte[BUFFER_SIZE];
        this.size = 0;
        this.offset = 0;
        this.startOffset = 0;
        this.spillBuffer = new GrowableByteBuffer();
        this.physicalRowNum = 0;
    }

    @Override
    public void grabNext(final ByteSlice dest, final MutableBoolean lastInRow,
            final MutableBoolean endOfInput) throws CsvReaderException {
        spillBuffer.clear();
        startOffset = offset;

        if (ignoreSurroundingSpaces) {
            skipWhitespace();
        }

        // Is first char the quote char?
        if (tryEnsureMore() && buffer[offset] == quoteChar) {
            ++offset;
            processQuotedMode(dest, lastInRow, endOfInput);
            if (trim) {
                ReaderUtil.trimSpacesAndTabs(dest);
            }
        } else {
            processUnquotedMode(dest, lastInRow, endOfInput);
            if (ignoreSurroundingSpaces) {
                ReaderUtil.trimSpacesAndTabs(dest);
            }
        }
    }

    /**
     * Process characters in "quoted mode". This involves some trickery to deal with quoted quotes and the end quote.
     *
     * @param lastInRow An out parameter. Its contents will be set to true if the cell just read was the last cell in
     *        the row, otherwise the contents will be set to false.
     */
    private void processQuotedMode(final ByteSlice dest, final MutableBoolean lastInRow,
            final MutableBoolean endOfInput) throws CsvReaderException {
        startOffset = offset;
        boolean prevCharWasCarriageReturn = false;
        outer: while (true) {
            if (!tryEnsureMore()) {
                throw new CsvReaderException("Cell did not have closing quote character");
            }

            // Advance through buffer while the characters are not special.
            // This tighter loop makes for more optimizer-friendly code.
            byte ch = buffer[offset];
            while (ch != '\n' && ch != '\r' && ch != escapeChar && ch != quoteChar) {
                ++offset;
                if (offset == size) {
                    // Refill buffer or finish.
                    continue outer;
                }
                ch = buffer[offset];
            }

            ++offset;

            // Maintain a correct row number. This is somewhat tricky.
            if (ch == '\r') {
                ++physicalRowNum;
                prevCharWasCarriageReturn = true;
                continue;
            }

            if (ch == '\n') {
                if (!prevCharWasCarriageReturn) {
                    ++physicalRowNum;
                }
                prevCharWasCarriageReturn = false;
                continue;
            }

            prevCharWasCarriageReturn = false;

            if (ch == escapeChar) {
                processEscapeChar();
                continue;
            }

            // This character is a quote char. It could be the end of the cell, or it could be an escaped
            // quote char (e.g. ""). The way to tell is to peek ahead at the next character.
            if (!tryEnsureMore()) {
                // There is no next char (we are at end of input), so let's call this end of cell.
                break;
            }
            final byte peek = buffer[offset];
            if (peek != quoteChar) {
                // There is a next char, but it's not a quotation mark. So this
                // quotation mark must be the end of the quoted string.
                break;
            }
            // There is a next character, and it *is* a quotation mark. So this is a quoted quote
            // "", to be interpreted as ". So we'll spill this string (up to the first quotation mark),
            // skip the second quotation mark, and keep going.
            spillRange();
            // Skip the second quotation mark.
            ++offset;
            startOffset = offset;
        }

        // The easiest way to make all the above logic run smoothly is to let the final quotation mark
        // (which will unconditionally be there) and subsequent matter (if any) into the field.
        // Then we can simply trim it back out, making sure that what we are trimming is only whitespace.
        // After trimming, we will see if the expected number of chars matches the actual number of chars.
        // The -1 here is because the number of characters processed includes the closing quote already.
        final int expectedSize = spillBuffer.size() + offset - startOffset - 1;
        finishField(dest, lastInRow, endOfInput);

        // Trim away any trailing whitespace
        while (dest.begin() != dest.end() && RangeTests.isSpaceOrTab(dest.back())) {
            dest.setEnd(dest.end() - 1);
        }

        final String exceptionMessage = "Logic error: final non-whitespace in field is not quoteChar";

        // Trim away the final quote char
        if (dest.begin() == dest.end() || dest.back() != quoteChar) {
            throw new RuntimeException(exceptionMessage);
        }
        dest.setEnd(dest.end() - 1);

        // Ensure we have the expected number of chars. The above logic can get misled if there are multiple
        // closing quotes, as in the input "hello there"junk".
        // The quote at the end of 'there' is the real closing quote; the remainder of the text is trash and should
        // be rejected.
        if (dest.size() != expectedSize) {
            throw new RuntimeException(exceptionMessage);
        }
    }

    /**
     * Process characters in "unquoted mode". This is easy: eat characters until the next field or line delimiter.
     */
    private void processUnquotedMode(final ByteSlice dest, final MutableBoolean lastInRow,
            final MutableBoolean endOfInput) throws CsvReaderException {
        startOffset = offset;
        finishField(dest, lastInRow, endOfInput);
    }

    /** Skip whitespace but do not consider the field delimiter to be whitespace. */
    private void skipWhitespace() throws CsvReaderException {
        while (true) {
            if (offset == size) {
                if (!tryEnsureMore()) {
                    return;
                }
            }
            final byte ch = buffer[offset];
            if (ch == fieldDelimiter || !RangeTests.isSpaceOrTab(ch)) {
                return;
            }
            ++offset;
        }
    }

    /**
     * Eat characters until the next field or line delimiter.
     *
     * @param lastInRow An out parameter. Its contents are set to true if the cell was the last one in the row.
     *        Otherwise, its contents are set to false.
     */
    private void finishField(final ByteSlice dest, final MutableBoolean lastInRow,
            final MutableBoolean endOfInput)
            throws CsvReaderException {
        outer: while (true) {
            if (!tryEnsureMore()) {
                finish(dest);
                // End of input sets both flags.
                lastInRow.setValue(true);
                endOfInput.setValue(true);
                return;
            }

            // Advance through buffer while the characters are not special.
            // This tighter loop makes for more optimizer-friendly code.
            byte ch = buffer[offset];
            while (ch != fieldDelimiter && ch != '\n' && ch != '\r' && ch != escapeChar) {
                ++offset;
                if (offset == size) {
                    // Refill buffer or finish.
                    continue outer;
                }
                ch = buffer[offset];
            }

            if (ch == fieldDelimiter) {
                finish(dest);
                ++offset; // ... and skip over the field delimiter.
                lastInRow.setValue(false);
                endOfInput.setValue(false);
                return;
            }
            if (ch == '\n') {
                finish(dest);
                ++offset;
                lastInRow.setValue(true);
                endOfInput.setValue(false);
                ++physicalRowNum;
                return;
            }
            if (ch == '\r') {
                // This is slightly complicated because we have to look ahead for a possible \n.
                // The easiest way to deal with this is to let it into our slice and then trim it
                // off at the end.
                ++offset;
                int excess = 1;
                if (tryEnsureMore()) {
                    // might be \r\n
                    if (buffer[offset] == '\n') {
                        ++offset;
                        excess = 2;
                    }
                }
                finish(dest);
                dest.reset(dest.data(), dest.begin(), dest.end() - excess);
                lastInRow.setValue(true);
                endOfInput.setValue(false);
                ++physicalRowNum;
                return;
            }

            // ch is escapeChar
            ++offset;
            processEscapeChar();
        }
    }

    private void processEscapeChar() throws CsvReaderException {
        // Spill data up to and including the escape character into the spill buffer.
        // Below, we will replace the escape character with the transformed escaped character.
        spillRange();

        // This character is an escape character. In practice, it is used to either to make the next
        // metacharacter like the quote or field separator normal, or to provide a C-style special character like
        // newline or tab.
        // However, it can't appear as the last character of the input.
        if (!tryEnsureMore()) {
            throw new CsvReaderException("The escape character cannot be the last character of the input");
        }

        // Consume the next char (the escaped character). Potentially transform it if it is one of the C escapes:
        // characters b, t, n etc
        final byte nextChar = buffer[offset++];
        final byte nextCharTransformed = transformEscapedChar(nextChar);

        // Replace the placeholder character with the transformed character
        spillBuffer.data()[spillBuffer.size() - 1] = nextCharTransformed;

        // Advance the spill buffer's notion of "next start position" so it skips the escaped character.
        startOffset = offset;
    }


    /**
     * Interpret the set of character escapes supported by Java. We do not currently interpet the octal 0xx or Unicode
     * escape sequences uxxxx
     * 
     * @param nextChar The character following the escape character.
     * @return If one of (b, t, n, r, f), that value transformed to (\b, \t, \n, \r, \f). Otherwise, the value is
     *         returned unchanged.
     * @throws CsvReaderException if passed a non-ASCII character, carriage return, or newline.
     */
    private static byte transformEscapedChar(byte nextChar) throws CsvReaderException {
        if (nextChar < 0) {
            throw new CsvReaderException("Can't escape a non-ASCII character");
        }
        if (nextChar == '\r' || nextChar == '\n') {
            throw new CsvReaderException("Can't escape a carriage return or newline");
        }
        switch (nextChar) {
            case 'b':
                return '\b';
            case 't':
                return '\t';
            case 'n':
                return '\n';
            case 'r':
                return '\r';
            case 'f':
                return '\f';
            default:
                return nextChar;
        }
    }

    /** @return true if there are more characters. */
    private boolean tryEnsureMore() throws CsvReaderException {
        if (offset != size) {
            return true;
        }
        spillRange();
        refillBuffer();
        return size != 0;
    }

    /**
     * Spill the current range to the spillBuffer. Normally we try to stay in the "common case", where the entire cell
     * we are reading is consecutive characters in the underlying input buffer. This assumption fails when either there
     * are escaped quotes (like "" needing to be interpreted as "), or when the cell we are reading spans the boundaries
     * of two input buffers. In that case we "spill" the characters we have collected so far to the spillBuffer.
     */
    private void spillRange() {
        spillBuffer.append(buffer, startOffset, offset - startOffset);
        startOffset = offset;
    }

    /** Get another chunk of data from the Reader. */
    private void refillBuffer() throws CsvReaderException {
        offset = 0;
        startOffset = 0;
        try {
            final int bytesRead = inputStream.read(buffer, 0, buffer.length);
            if (bytesRead < 0) {
                size = 0;
                return;
            }
            if (bytesRead > 0) {
                size = bytesRead;
                return;
            }
            throw new CsvReaderException("Logic error: zero-length read");
        } catch (IOException inner) {
            throw new CsvReaderException("Caught exception", inner);
        }
    }

    private void finish(final ByteSlice dest) {
        if (spillBuffer.size() == 0) {
            // If we never spilled then our whole output is in the input buffer. So we can
            // just return a slice of the input buffer.
            dest.reset(buffer, startOffset, offset);
            return;
        }
        // Otherwise, append we need to append whatever residual is left to spillBuffer
        // and return a slice of spillBuffer.
        spillRange();
        dest.reset(spillBuffer.data(), 0, spillBuffer.size());
    }

    public int physicalRowNum() {
        return physicalRowNum;
    }
}
