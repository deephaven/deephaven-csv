package io.deephaven.csv.reading.cells;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.reading.ReaderUtil;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.csv.util.MutableBoolean;
import io.deephaven.csv.util.MutableInt;

import java.io.InputStream;

/**
 * This class uses an underlying DelimitedCellGrabber to grab whole lines at a time from the input stream, and then it
 * breaks them into fixed-sized cells to return to the caller.
 */
public class FixedCellGrabber implements CellGrabber {
    /**
     * Makes a degenerate CellGrabber that has no delimiters or quotes and therefore returns whole lines. This is a
     * somewhat quick-and-dirty way to reuse the buffering and newline logic in DelimitedCellGrabber without rewriting
     * it.
     * 
     * @param stream The underlying stream.
     * @return The "line grabber"
     */
    public static CellGrabber makeLineGrabber(InputStream stream) {
        final byte IllegalUtf8 = (byte) 0xff;
        return new DelimitedCellGrabber(stream, IllegalUtf8, IllegalUtf8, true, false);
    }

    private final CellGrabber lineGrabber;
    private final int[] columnWidths;
    private final boolean ignoreSurroundingSpaces;
    private final boolean utf32CountingMode;
    private final ByteSlice rowText;
    private boolean needsUnderlyingRefresh;
    private int colIndex;
    private final MutableBoolean dummy1;
    private final MutableInt dummy2;

    /** Constructor. */
    public FixedCellGrabber(final CellGrabber lineGrabber, final int[] columnWidths, boolean ignoreSurroundingSpaces,
            boolean utf32CountingMode) {
        this.lineGrabber = lineGrabber;
        this.columnWidths = columnWidths;
        this.ignoreSurroundingSpaces = ignoreSurroundingSpaces;
        this.utf32CountingMode = utf32CountingMode;
        this.rowText = new ByteSlice();
        this.needsUnderlyingRefresh = true;
        this.colIndex = 0;
        this.dummy1 = new MutableBoolean();
        this.dummy2 = new MutableInt();
    }

    @Override
    public void grabNext(ByteSlice dest, MutableBoolean lastInRow, MutableBoolean endOfInput)
            throws CsvReaderException {
        if (needsUnderlyingRefresh) {
            // Underlying row used up, and all columns provided. Ask underlying CellGrabber for the next line.
            lineGrabber.grabNext(rowText, dummy1, endOfInput);

            if (endOfInput.booleanValue()) {
                // Set dest to the empty string, and leave 'endOfInput' set to true.
                dest.reset(rowText.data(), rowText.end(), rowText.end());
                return;
            }

            needsUnderlyingRefresh = false;
            colIndex = 0;
        }

        // There is data to return. Count off N characters. The final column gets all remaining characters.
        final boolean lastCol = colIndex == columnWidths.length - 1;
        final int numCharsToTake = lastCol ? Integer.MAX_VALUE : columnWidths[colIndex];
        takeNCharactersInCharset(rowText, dest, numCharsToTake, utf32CountingMode, dummy2);
        ++colIndex;
        needsUnderlyingRefresh = lastCol || dest.size() == 0;
        lastInRow.setValue(needsUnderlyingRefresh);
        endOfInput.setValue(false);

        if (ignoreSurroundingSpaces) {
            ReaderUtil.trimSpacesAndTabs(dest);
        }
    }

    private static void takeNCharactersInCharset(ByteSlice src, ByteSlice dest, int numCharsToTake,
            boolean utf32CountingMode, MutableInt tempInt) {
        final byte[] data = src.data();
        final int cellBegin = src.begin();
        int current = cellBegin;
        while (numCharsToTake > 0) {
            if (current == src.end()) {
                break;
            }
            final int utf8Length = ReaderUtil.getUtf8LengthAndCharLength(data[current], src.end() - current,
                    utf32CountingMode, tempInt);
            if (numCharsToTake < tempInt.intValue()) {
                // There is not enough space left in the field to store this character.
                // This can happen if CsvSpecs is set for the UTF16 counting convention,
                // there is one unit left in the field, and we encounter a character outside
                // the Basic Multilingual Plane, which would require two units.
                break;
            }
            numCharsToTake -= tempInt.intValue();
            current += utf8Length;
        }
        dest.reset(src.data(), cellBegin, current);
        src.reset(src.data(), current, src.end());
    }

    @Override
    public int physicalRowNum() {
        return lineGrabber.physicalRowNum();
    }
}
