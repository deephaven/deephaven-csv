package io.deephaven.csv.reading.headers;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.reading.ReaderUtil;
import io.deephaven.csv.reading.cells.CellGrabber;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.csv.util.MutableBoolean;
import io.deephaven.csv.util.MutableObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A static class which contains one method: "determineHeadersToUse", which looks at the specs and the input and
 * determines what should constitute the header row (perhaps caller-supplied, perhaps present in the input, etc.)
 */
public class DelimitedHeaderFinder {
    /**
     * Determine which headers to use. The result comes from either the first row of the file or the user-specified
     * overrides.
     * 
     * @param specs The CsvSpecs
     * @param grabber The CellGrabber
     * @param firstDataRowHolder An output parameter where we store the first data row. We need this because we might
     *        have eagerly read the first row, so we have to give it to the caller to subsequently process as data.
     * @return The headers to use.
     * @throws CsvReaderException if the input is malformed
     */
    public static String[] determineHeadersToUse(final CsvSpecs specs,
            final CellGrabber grabber, final MutableObject<byte[][]> firstDataRowHolder)
            throws CsvReaderException {
        String[] headersToUse = null;
        if (specs.hasHeaderRow()) {
            long skipCount = specs.skipHeaderRows();
            byte[][] headerRow;
            while (true) {
                headerRow = tryReadOneRow(grabber);
                if (headerRow == null) {
                    throw new CsvReaderException(
                            "Can't proceed because hasHeaderRow is set but input file is empty or shorter than skipHeaderRows");
                }
                if (skipCount == 0) {
                    break;
                }
                --skipCount;
            }
            headersToUse = Arrays.stream(headerRow).map(x -> new String(x, specs.charset())).toArray(String[]::new);
        }

        // Whether or not the input had headers, maybe override with client-specified headers.
        if (specs.headers().size() != 0) {
            headersToUse = specs.headers().toArray(new String[0]);
        }

        // If we still have nothing, try to generate synthetic column headers (works only if the file is
        // non-empty, because we need to infer the column count).
        final byte[][] firstDataRow;
        if (headersToUse == null) {
            firstDataRow = tryReadOneRow(grabber);
            if (firstDataRow == null) {
                throw new CsvReaderException(
                        "Can't proceed because input file is empty and client has not specified headers");
            }
            headersToUse = ReaderUtil.makeSyntheticHeaders(firstDataRow.length);
        } else {
            firstDataRow = null;
        }

        // Apply column specific overrides.
        for (Map.Entry<Integer, String> entry : specs.headerForIndex().entrySet()) {
            headersToUse[entry.getKey()] = entry.getValue();
        }

        firstDataRowHolder.setValue(firstDataRow);
        return headersToUse;
    }

    /**
     * Try to read one row from the input. Returns null if the input is empty
     * 
     * @param grabber The CellGrabber
     * @return The first row as a byte[][] or null if the input was exhausted.
     */
    private static byte[][] tryReadOneRow(final CellGrabber grabber) throws CsvReaderException {
        final List<byte[]> headers = new ArrayList<>();

        // Grab the header
        final ByteSlice slice = new ByteSlice();
        final MutableBoolean lastInRow = new MutableBoolean();
        final MutableBoolean endOfInput = new MutableBoolean();
        do {
            grabber.grabNext(slice, lastInRow, endOfInput);
            final byte[] item = new byte[slice.size()];
            slice.copyTo(item, 0);
            headers.add(item);
        } while (!lastInRow.booleanValue());
        if (headers.size() == 1 && headers.get(0).length == 0 && endOfInput.booleanValue()) {
            return null;
        }
        return headers.toArray(new byte[0][]);
    }
}
