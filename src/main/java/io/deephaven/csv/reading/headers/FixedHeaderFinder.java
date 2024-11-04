package io.deephaven.csv.reading.headers;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.reading.ReaderUtil;
import io.deephaven.csv.reading.cells.CellGrabber;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.csv.util.MutableBoolean;
import io.deephaven.csv.util.MutableInt;
import io.deephaven.csv.util.MutableObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FixedHeaderFinder {
    /**
     * Determine which headers to use. The result comes from either the first row of the file or the user-specified
     * overrides.
     */
    public static String[] determineHeadersToUse(
            final CsvSpecs specs,
            final CellGrabber lineGrabber,
            MutableObject<int[]> columnWidthsResult)
            throws CsvReaderException {
        String[] headersToUse;
        // Get user-specified column widths, if any. If none were specified, this will be an array of length 0.
        // The column widths are in units of the specified convention (either UTF-16 or UTF-32 units).
        int[] columnWidthsToUse = specs.fixedColumnWidths().stream().mapToInt(Integer::intValue).toArray();
        if (specs.hasHeaderRow()) {
            long skipCount = specs.skipHeaderRows();
            final ByteSlice headerRow = new ByteSlice();
            MutableBoolean lastInRow = new MutableBoolean();
            MutableBoolean endOfInput = new MutableBoolean();
            while (true) {
                lineGrabber.grabNext(headerRow, lastInRow, endOfInput);
                if (endOfInput.booleanValue()) {
                    throw new CsvReaderException(
                            "Can't proceed because hasHeaderRow is set but input file is empty or shorter than skipHeaderRows");
                }
                if (skipCount == 0) {
                    break;
                }
                --skipCount;
            }
            if (columnWidthsToUse.length == 0) {
                columnWidthsToUse = inferColumnWidths(headerRow, specs.useUtf32CountingConvention());
            }

            headersToUse =
                    extractHeaders(headerRow, columnWidthsToUse, specs.useUtf32CountingConvention());
        } else {
            if (columnWidthsToUse.length == 0) {
                throw new CsvReaderException(
                        "Can't proceed because hasHeaderRow is false but fixedColumnWidths is unspecified");
            }
            headersToUse = ReaderUtil.makeSyntheticHeaders(columnWidthsToUse.length);
        }

        // Whether or not the input had headers, maybe override with client-specified headers.
        if (specs.headers().size() != 0) {
            if (specs.headers().size() != headersToUse.length) {
                final String message = String.format("Library determined %d headers; caller overrode with %d headers",
                        headersToUse.length, specs.headers().size());
                throw new CsvReaderException(message);
            }
            headersToUse = specs.headers().toArray(new String[0]);
        }

        // Apply column specific overrides.
        for (Map.Entry<Integer, String> entry : specs.headerForIndex().entrySet()) {
            headersToUse[entry.getKey()] = entry.getValue();
        }

        columnWidthsResult.setValue(columnWidthsToUse);
        return headersToUse;
    }

    /**
     * Infer the column widths by looking for the transition from delimiter char to non-delimiter char.
     * 
     * @param row The input row
     * @param useUtf32CountingConvention The character set convention we are using for units of width (either UTF-32 or
     *        UTF-16)
     * @return The widths of the columns, in the specified character set convention.
     */
    private static int[] inferColumnWidths(ByteSlice row, boolean useUtf32CountingConvention) {
        // A column start is a non-delimiter character preceded by a delimiter (or present at the start of line).
        // If the start of the line is a delimiter, that is an error.
        final List<Integer> columnWidths = new ArrayList<>();
        final MutableInt charCountResult = new MutableInt();
        boolean prevCharIsSpace = false;
        final byte[] data = row.data();
        int numChars = 0;
        int currentIndex = row.begin();
        while (true) {
            if (currentIndex == row.end()) {
                columnWidths.add(numChars);
                return columnWidths.stream().mapToInt(Integer::intValue).toArray();
            }
            // If this character is not a delimiter, but the previous one was, then this is the start of a new column.
            byte ch = data[currentIndex];
            boolean thisCharIsSpace = ch == ' ';
            if (currentIndex == row.begin() && thisCharIsSpace) {
                throw new IllegalArgumentException("Header row cannot start with a space");
            }
            if (!thisCharIsSpace && prevCharIsSpace) {
                columnWidths.add(numChars);
                numChars = 0;
            }
            prevCharIsSpace = thisCharIsSpace;
            final int utf8Length = ReaderUtil.getUtf8LengthAndCharLength(ch, row.end() - currentIndex,
                    useUtf32CountingConvention, charCountResult);
            currentIndex += utf8Length;
            numChars += charCountResult.intValue();
        }
    }

    /**
     * Extract the headers names from 'row'.
     * 
     * @param row The header row
     * @param columnWidths The width of the columns, in the UTF-32 or UTF-16 counting convention.
     * @param utf32CountingMode Whether we are in the UTF-32 or UTF-16 counting mode
     * @return The array of headers
     */
    private static String[] extractHeaders(ByteSlice row, int[] columnWidths, boolean utf32CountingMode) {
        final int numCols = columnWidths.length;
        if (numCols == 0) {
            return new String[0];
        }
        final int[] byteWidths = new int[numCols];
        final ByteSlice tempSlice = new ByteSlice();
        final int excessBytes = charWidthsToByteWidths(row, columnWidths, utf32CountingMode, byteWidths);
        // Our policy is that the last column gets any excess bytes that are in the row.
        byteWidths[numCols - 1] += excessBytes;
        final String[] result = new String[numCols];

        int beginByte = row.begin();
        for (int colNum = 0; colNum != numCols; ++colNum) {
            final int proposedEndByte = beginByte + byteWidths[colNum];
            final int actualEndByte = Math.min(proposedEndByte, row.end());
            tempSlice.reset(row.data(), beginByte, actualEndByte);
            ReaderUtil.trimSpacesAndTabs(tempSlice);
            result[colNum] = tempSlice.toString();
            beginByte = actualEndByte;
        }
        return result;
    }

    /**
     * Convert character widths to UTF-8 widths. This converts the character widths, which are in the specified
     * convention (either UTF-16 or UTF-32), which are fixed for the whole input, and which are determined by reading
     * the headers (or specified by the user), into UTF-8 widths, which are specific to this row. For example if a
     * charWidth is 2 and the utf32CountingMode is true, then we need to scan the row for the next two Unicode
     * characters and count how many UTF-8 bytes that took up.
     * 
     * @param row The row we are processing
     * @param charWidths The column widths, in units of UTF-32 or UTF-16 units.
     * @param utf32CountingMode Whether we are counting in UTF-32 or UTF-16 mode
     * @param byteWidths The corresponding number of UTF-8 bytes corresponding to the charWidths for this row.
     * @return The number of excess UTF-8 bytes in this row that go beyond all the charWidths.
     */
    private static int charWidthsToByteWidths(ByteSlice row, int[] charWidths, boolean utf32CountingMode,
            int[] byteWidths) {
        int numCols = charWidths.length;
        if (byteWidths.length != numCols) {
            throw new IllegalArgumentException(
                    String.format("Expected charWidths.length (%d) == byteWidths.length (%d)",
                            charWidths.length, byteWidths.length));
        }
        final MutableInt charCountResult = new MutableInt();
        final byte[] data = row.data();
        int start = row.begin();
        int current = start;
        int colIndex = 0;
        int charCount = 0;
        while (true) {
            if (colIndex == numCols) {
                // Excess bytes not claimed by any column
                return row.end() - current;
            }
            if (charCount == charWidths[colIndex]) {
                byteWidths[colIndex] = current - start;
                start = current;
                charCount = 0;
                ++colIndex;
                continue;
            }

            final byte ch = data[current];
            final int utf8Length = ReaderUtil.getUtf8LengthAndCharLength(ch, row.end() - current, utf32CountingMode,
                    charCountResult);
            current += utf8Length;
            charCount += charCountResult.intValue();
        }
    }
}
