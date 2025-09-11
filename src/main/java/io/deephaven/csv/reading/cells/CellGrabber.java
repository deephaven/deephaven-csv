package io.deephaven.csv.reading.cells;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.csv.util.MutableBoolean;

/**
 * This class is used to traverse over text from a Reader, understanding both field and line delimiters, as well as the
 * CSV quoting convention, and breaking the text into cells for use by the calling code.
 */
public interface CellGrabber {
    /**
     * Try to grab the next cell from the input, being aware of field delimiters, line delimiters, quoting, and
     * trimming.
     *
     * @param dest The result, as a {@link ByteSlice}. The ByteSlice is invalidated by the next call to grabNext.
     * @param lastInRow An out parameter which will be set to true if the cell just read was the last cell in the row,
     *        otherwise it will be set to false.
     * @param endOfInput An out parameter which will be set to true if the cell just read encountered the end of the
     *        input, otherwise it will be set to false.
     * @throws CsvReaderException If the cell is malformed
     */
    void grabNext(final ByteSlice dest, final MutableBoolean lastInRow,
            final MutableBoolean endOfInput) throws CsvReaderException;

    /**
     * Returns the "physical" row number, that is the row number of the input file. This differs from the "logical" row
     * number, which is the row number of the CSV data being processed. The difference arises when, due to quotation
     * marks, a single CSV row can span multiple lines of input.
     * 
     * @return the "physical" row number
     */
    int physicalRowNum();
}
