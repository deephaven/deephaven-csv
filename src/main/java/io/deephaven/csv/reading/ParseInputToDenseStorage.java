package io.deephaven.csv.reading;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.densestorage.DenseStorageReader;
import io.deephaven.csv.densestorage.DenseStorageWriter;
import io.deephaven.csv.reading.cells.CellGrabber;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.csv.util.MutableBoolean;

import java.nio.charset.StandardCharsets;

/**
 * The job of this class is to take the input text, parse the CSV format (dealing with quoting, escaping, field
 * delimiters, and line delimiters) in order to break it into columns of cells (where a cell just contains uninterpreted
 * text... we haven't yet tried to parse into types yet), and to feed each of those columns of cells into its own
 * {@link DenseStorageWriter}. On the reading side, there is a {@link DenseStorageReader} paired with every
 * {@link DenseStorageWriter} and its job is to pull the data back out and have it processed by the
 * {@link ParseDenseStorageToColumn} class. The job of that class is to do pick the most appropriate parser, typically
 * by doing type inference, and parse the text into typed data. The reason for all this separation is that the
 * {@link DenseStorageReader} and {@link ParseDenseStorageToColumn} classes can run concurrently for each column.
 */
public class ParseInputToDenseStorage {
    /**
     * Take cell text (parsed by the {@link CellGrabber}), and feed them to the various {@link DenseStorageWriter}
     * classes.
     *
     * @param columnHeaders The column headers.
     * @param optionalFirstDataRow If not null, this is the first row of data from the file, which the caller had to
     *        peek at in order to know the number of columns in the file.
     * @param grabber The {@link CellGrabber} which does all the CSV format handling (delimiters, quotes, etc).
     * @param dsws The array of {@link DenseStorageWriter}s, one for each column. As a special case, if a given
     *        {@link DenseStorageWriter} is null, then instead of passing data to it, we confirm that the data is the
     *        empty string and then just drop the data. This is used to handle input files that have a trailing empty
     *        column on the right.
     * @param specs The {@link CsvSpecs} which control how the CSV file is interpreted.
     * @return The number of data rows in the input (i.e. not including headers or strings split across multiple lines).
     */
    public static long doit(final String[] columnHeaders,
            final byte[][] optionalFirstDataRow,
            final CellGrabber grabber,
            final CsvSpecs specs,
            final String[][] nullValueLiteralsToUse,
            final DenseStorageWriter[] dsws)
            throws CsvReaderException {
        // This is the number of data rows read.
        long numProcessedRows = 0;

        final RowAppender rowAppender =
                new RowAppender(columnHeaders, optionalFirstDataRow, grabber, specs, nullValueLiteralsToUse, dsws);
        long skipRows = specs.skipRows();
        while (skipRows != 0) {
            final RowResult result = rowAppender.processNextRow(false);
            if (result == RowResult.END_OF_INPUT) {
                break;
            }
            --skipRows;
        }

        long numRows = specs.numRows();
        while (numRows != 0) {
            final RowResult result = rowAppender.processNextRow(true);
            if (result == RowResult.END_OF_INPUT) {
                break;
            }
            if (result == RowResult.PROCESSED_ROW) {
                ++numProcessedRows;
            }
            // PROCESSED_ROW OR IGNORED_EMPTY_ROW
            --numRows;
        }

        for (DenseStorageWriter dsw : dsws) {
            if (dsw != null) {
                dsw.finish();
            }
        }
        return numProcessedRows;
    }

    private enum RowResult {
        END_OF_INPUT, IGNORED_EMPTY_ROW, PROCESSED_ROW
    }

    private static class RowAppender {
        private final String[] columnHeaders;
        private byte[][] optionalFirstDataRow;
        private final CellGrabber grabber;
        private final DenseStorageWriter[] dsws;
        private final CsvSpecs specs;
        private final int numCols;
        private final ByteSlice byteSlice;
        private final MutableBoolean lastInRow;
        private final MutableBoolean endOfInput;
        private final byte[][] nullValueLiteralsAsUtf8;

        public RowAppender(final String[] columnHeaders, final byte[][] optionalFirstDataRow, final CellGrabber grabber,
                final CsvSpecs specs, final String[][] nullValueLiteralsToUse, final DenseStorageWriter[] dsws)
                throws CsvReaderException {
            this.columnHeaders = columnHeaders;
            this.optionalFirstDataRow = optionalFirstDataRow;
            this.grabber = grabber;
            this.dsws = dsws;
            this.specs = specs;
            numCols = dsws.length;
            if (optionalFirstDataRow != null && optionalFirstDataRow.length != numCols) {
                throw new CsvReaderException(
                        String.format(
                                "Expected %d columns but optionalFirstRow had %d",
                                numCols, optionalFirstDataRow.length));
            }
            byteSlice = new ByteSlice();
            lastInRow = new MutableBoolean();
            endOfInput = new MutableBoolean();
            // Here we prepare ahead of time what we are going to do when we encounter a short row. Say the input looks
            // like:
            // 10,20,30,40,50
            // 60,70,80
            // The "60,70,80" line is a short row; it provides no data for the fourth or fifth columns. If
            // CsvSpecs.allowMissingColumns is false, we would throw an exception at this point. However, if
            // it is true, then we behave as if the line actually had looked like this
            // 60,70,80,$NullLiteralFor4,$NullLiteralFor5
            // Here $NullLiteralFor...(4 or 5) are not the literal text "$NullLiteralFor4" and so on,
            // but rather the specific null value strings configured for the given column in CsvSpecs.
            // These are configured via CsvSpecs.nullValueLiterals(), CsvSpecs.nullValueLiteralsForName(),
            // and CsvSpecs.nullValueLiteralsForIndex(). By this point the interpretation as already been done
            // and the answers have been passed to this method via 'nullValueLiteralsToUse'. Our job here is
            // to pick the right null value literal and convert it into UTF8 for slightly faster processing down
            // the line, in the code RowAppender.processNextRow(). There are three cases. For a given column i:
            // 1. If there is no null literal configured for column i, leave the Java value null here. When
            // RowAppender.processNextRow() sees a short row that includes this missing column, it will throw
            // an exception.
            // 2. If there is exactly one null literal configured for column i, use it.
            // When RowAppender.processNextRow() sees a short row that includes this missing column, it will
            // provide this null literal text instead.
            // 3. If there is more than one null literal configured for column i, arbitrarily choose one (say,
            // the first). Then the code behaves as if it is in case 2.
            //
            // Example:
            // Column 4 is configured to accept the single null value literal "NoCustomerData".
            // Column 5 is configured to accept multiple null literals "N/A", "null", "Null", and "NULL"
            // In our example we would interpret the short row as if it had read
            // 60,70,80,NoCustomerData,N/A
            //
            // Then, code in ParseDenseStorageToColumn would see these strings "NoCustomerData" and "N/A" and
            // interpret them as nulls for that column. Note that for column 5 we could have chosen any of the four
            // available null tokens, and regardless of which one we chose, the reader in ParseDenseStorageToColumn
            // would have interpreted our choice as the null element value for that column, because they are
            // interchangeable.
            nullValueLiteralsAsUtf8 = new byte[nullValueLiteralsToUse.length][];
            for (int ii = 0; ii < nullValueLiteralsToUse.length; ++ii) {
                final String[] nvls = nullValueLiteralsToUse[ii];
                if (nvls.length != 0) {
                    nullValueLiteralsAsUtf8[ii] = nvls[0].getBytes(StandardCharsets.UTF_8);
                }
                // otherwise leave that slot as null.
            }
        }

        /**
         * @param writeToConsumer If true, write the data to the sink. If false, just consume the data but don't write
         *        anything. This is used to implement the "skip rows" functionality.
         * @return true if a row was consumed, false otherwise.
         */
        public RowResult processNextRow(final boolean writeToConsumer) throws CsvReaderException {
            if (optionalFirstDataRow != null) {
                for (int ii = 0; ii < numCols; ++ii) {
                    final byte[] temp = optionalFirstDataRow[ii];
                    byteSlice.reset(temp, 0, temp.length);
                    appendToDenseStorageWriter(dsws[ii], byteSlice, writeToConsumer);
                }
                optionalFirstDataRow = null;
                return RowResult.PROCESSED_ROW;
            }

            final int physicalRowNum = grabber.physicalRowNum();
            int colNum = 0;
            for (colNum = 0; colNum < numCols; ++colNum) {
                try {
                    grabber.grabNext(byteSlice, lastInRow, endOfInput);
                    if (lastInRow.booleanValue()) {
                        if (colNum == 0 && byteSlice.size() == 0) {
                            if (endOfInput.booleanValue()) {
                                return RowResult.END_OF_INPUT;
                            }
                            if (specs.ignoreEmptyLines()) {
                                return RowResult.IGNORED_EMPTY_ROW;
                            }
                        }
                        appendToDenseStorageWriter(dsws[colNum], byteSlice, writeToConsumer);
                        ++colNum;
                        break;
                    }
                    appendToDenseStorageWriter(dsws[colNum], byteSlice, writeToConsumer);
                } catch (Exception e) {
                    final String message = String.format("While processing row %d, column %s:", physicalRowNum + 1,
                            describeColumnHeader(columnHeaders, colNum));
                    throw new CsvReaderException(message, e);
                }
            }
            if (!lastInRow.booleanValue()) {
                // There are excess columns. Either complain about them or eat them.
                if (!specs.ignoreExcessColumns()) {
                    // Complain.
                    final String message = String.format(
                            "Row %d has too many columns (expected %d)", physicalRowNum + 1, numCols);
                    throw new CsvReaderException(message);
                }
                // Eat.
                while (!lastInRow.booleanValue()) {
                    grabber.grabNext(byteSlice, lastInRow, endOfInput);
                }
            }

            if (colNum >= numCols) {
                return RowResult.PROCESSED_ROW;
            }

            // If "short rows" are not allowed, throw an exception.
            if (!specs.allowMissingColumns()) {
                final String message = String.format(
                        "Row %d has too few columns (expected %d)", physicalRowNum + 1, numCols);
                throw new CsvReaderException(message);
            }

            // Pad the row with a null value literal appropriate for each column.
            while (colNum < numCols) {
                final byte[] nvl = nullValueLiteralsAsUtf8[colNum];
                if (nvl == null) {
                    final String message = String.format(
                            "Row %d is short, but can't null-fill it because there is no configured null value literal for column %s.",
                            physicalRowNum + 1, describeColumnHeader(columnHeaders, colNum));
                    throw new CsvReaderException(message);
                }
                byteSlice.reset(nvl, 0, nvl.length);
                appendToDenseStorageWriter(dsws[colNum], byteSlice, writeToConsumer);
                ++colNum;
            }
            return RowResult.PROCESSED_ROW;
        }
    }

    /**
     *
     * @param dsw The DenseStorageWriter to write to.
     * @param bs The ByteSlice containing the data.
     * @param writeToConsumer If true, perform the operation. If false, do nothing. This flag makes it easy to
     *        concentrate the "skip rows" logic in one place.
     */
    private static void appendToDenseStorageWriter(final DenseStorageWriter dsw, final ByteSlice bs,
            boolean writeToConsumer)
            throws CsvReaderException {
        if (!writeToConsumer) {
            return;
        }
        if (dsw != null) {
            dsw.append(bs);
            return;
        }
        if (bs.size() != 0) {
            throw new CsvReaderException("Column assumed empty but contains data");
        }
    }

    /**
     * Return either the specified header name in quotes, or the string "(Column N)" if we don't have a header name for
     * the specified colNum. This latter case can happen if the file has trailing delimiters, which we treat as a kind
     * of shadow column with no data, and if there's an error in that column (i.e. if some data shows up in some row of
     * that column, which is required to be completely empty).
     */
    private static String describeColumnHeader(final String[] columnHeaders, final int colNum) {
        if (colNum < columnHeaders.length) {
            return '"' + columnHeaders[colNum] + '"';
        }
        return String.format("(Column %d)", colNum);
    }
}
