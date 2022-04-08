package io.deephaven.csv.reading;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.densestorage.DenseStorageReader;
import io.deephaven.csv.densestorage.DenseStorageWriter;
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
    public static long doit(final byte[][] optionalFirstDataRow,
            final CellGrabber grabber,
            final CsvSpecs specs,
            final String[][] nullValueLiteralsToUse,
            final DenseStorageWriter[] dsws)
            throws CsvReaderException {
        // This is the number of data rows read.
        long numProcessedRows = 0;

        final RowAppender rowAppender =
                new RowAppender(optionalFirstDataRow, grabber, specs, nullValueLiteralsToUse, dsws);
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
        private byte[][] optionalFirstDataRow;
        private final CellGrabber grabber;
        private final DenseStorageWriter[] dsws;
        private final CsvSpecs specs;
        private final int numCols;
        private final ByteSlice byteSlice;
        private final MutableBoolean lastInRow;
        private final ByteSlice[] nullValueLiteralsAsSlices;

        public RowAppender(final byte[][] optionalFirstDataRow, final CellGrabber grabber, final CsvSpecs specs,
                final String[][] nullValueLiteralsToUse, final DenseStorageWriter[] dsws) throws CsvReaderException {
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
            // In case we need to null-fill short columns, we need a suitable null value literal for each column.
            // Note that the caller can specify zero, one, or more null value literals, and they can do so on a
            // per-column basis. If there is no suitable null value literal for a column, we calculate a null here.
            // Otherwise, if there are one or more suitable null value literals for a column, we arbitrarily take the
            // first one. In this latter case it doesn't matter to us which configured null value literal we use
            // for a column, because they should all be interchangeable.
            nullValueLiteralsAsSlices = new ByteSlice[nullValueLiteralsToUse.length];
            for (int ii = 0; ii < nullValueLiteralsToUse.length; ++ii) {
                final String[] nvls = nullValueLiteralsToUse[ii];
                if (nvls.length != 0) {
                    final byte[] nullValueBytes = nvls[0].getBytes(StandardCharsets.UTF_8);
                    nullValueLiteralsAsSlices[ii] = new ByteSlice(nullValueBytes, 0, nullValueBytes.length);
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
                    if (!grabber.grabNext(byteSlice, lastInRow)) {
                        if (colNum == 0) {
                            // Input exhausted
                            return RowResult.END_OF_INPUT;
                        }
                        // Can't get here. If there is any data at all in the last row, and *then* the file
                        // ends, grabNext() will return true, with lastInRow set.
                        throw new RuntimeException("Logic error: uncaught short last row");
                    }
                    if (lastInRow.booleanValue()) {
                        if (byteSlice.size() == 0 && colNum == 0 && specs.ignoreEmptyLines()) {
                            return RowResult.IGNORED_EMPTY_ROW;
                        }
                        appendToDenseStorageWriter(dsws[colNum], byteSlice, writeToConsumer);
                        ++colNum;
                        break;
                    }
                    appendToDenseStorageWriter(dsws[colNum], byteSlice, writeToConsumer);
                } catch (Exception e) {
                    final String message =
                            String.format("While processing row %d, column %d:", physicalRowNum + 1, colNum + 1);
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
                    if (!grabber.grabNext(byteSlice, lastInRow)) {
                        // Can't happen. Won't get end of input while finishing excess row.
                        throw new RuntimeException("Logic error: end of input while finishing excess row");
                    }
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

            // Pad the row with a null vlaue literal appropriate for each column.
            while (colNum < numCols) {
                final ByteSlice nullSlice = nullValueLiteralsAsSlices[colNum];
                if (nullSlice == null) {
                    final String message = String.format(
                            "Row %d is short, but can't null-fill it because there is no configured null value literal for column %d.",
                            physicalRowNum + 1, colNum + 1);
                    throw new CsvReaderException(message);
                }
                appendToDenseStorageWriter(dsws[colNum], nullSlice, writeToConsumer);
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
}
