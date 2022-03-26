package io.deephaven.csv.reading;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.densestorage.DenseStorageReader;
import io.deephaven.csv.densestorage.DenseStorageWriter;
import io.deephaven.csv.parsers.DataType;
import io.deephaven.csv.parsers.Parser;
import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.csv.util.MutableBoolean;
import io.deephaven.csv.util.MutableObject;
import io.deephaven.csv.util.Renderer;
import org.jetbrains.annotations.NotNull;

import java.io.InputStream;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.*;

/**
 * A class for reading CSV data. Typical usage is:
 *
 * <ol>
 * <li>Construct a CsvReader.
 * <li>Customize the CsvReader by calling the various setXXX methods.
 * <li>Arrange for the input text to be in a {@link Reader}.
 * <li>Prepare a {@link SinkFactory} which can in turn provide Sink&lt;T&gt; objects for the output data.
 * <li>Call the {@link #read} method.
 * </ol>
 *
 * Furthermore the setXXX methods can be used in a builder pattern. Example:
 *
 * <pre>
 * final CsvReader csvr = new CsvReader()
 *   .setQuoteChar('#')
 *   .setAsync(false)
 *   .setParserFor("Timestamp", Parsers.DATETIME);
 * final Reader r = ...;
 * final SinkFactory f = ...;
 * final CsvReader.Result res = csvr.read(r, f);
 * </pre>
 */
public final class CsvReader {
    /**
     * Utility class. Do not instantiate.
     */
    private CsvReader() {}

    /**
     * Read the data.
     *
     * @param specs A {@link CsvSpecs} object providing options for the parse.
     * @param stream The input data, encoded in UTF-8.
     * @param sinkFactory A factory that can provide Sink&lt;T&gt; of all appropriate types for the output data. Once
     *        the CsvReader determines what the column type is, it will use the {@link SinkFactory} to create an
     *        appropriate Sink&lt;T&gt; for the type. Note that the CsvReader might guess wrong, so it might create a
     *        Sink, partially populate it, and then abandon it. The final set of fully-populated Sinks will be returned
     *        in in the CsvReader.Result. Thread safety: The {@link SinkFactory} may be invoked concurrently, therefore
     *        it must be thread safe.
     * @return A CsvReader.Result containing the column names, the number of columns, and the final set of
     *         fully-populated Sinks.
     */
    public static Result read(final CsvSpecs specs, final InputStream stream, final SinkFactory sinkFactory)
            throws CsvReaderException {
        final byte quoteAsByte = check7BitAscii("quote", specs.quote());
        final byte delimiterAsByte = check7BitAscii("delimiter", specs.delimiter());
        final CellGrabber grabber =
                new CellGrabber(stream, quoteAsByte, delimiterAsByte, specs.ignoreSurroundingSpaces(),
                        specs.trim());
        // For an "out" parameter
        final MutableObject<byte[][]> firstDataRowHolder = new MutableObject<>();
        final String[] headersTemp = determineHeadersToUse(specs, grabber, firstDataRowHolder);
        final byte[][] firstDataRow = firstDataRowHolder.getValue();
        final int numInputCols = headersTemp.length;

        // If the final column in the header row is blank, we assume that the final column in all the data rows
        // is also blank (we confirm this assumption in ParseInputToDenseStorage, as we're reading the file).
        // This corresponds to a file with trailing delimiters.
        final String[] headersTemp2;
        if (numInputCols != 0 && headersTemp[numInputCols - 1].isEmpty()) {
            headersTemp2 = Arrays.copyOf(headersTemp, numInputCols - 1);
        } else {
            headersTemp2 = headersTemp;
        }
        final int numOutputCols = headersTemp2.length;
        final String[] headersToUse = canonicalizeHeaders(specs, headersTemp2);

        // Create a DenseStorageWriter and two readers for each column.
        final DenseStorageWriter[] dsws = new DenseStorageWriter[numInputCols];
        final DenseStorageReader[] dsr0s = new DenseStorageReader[numInputCols];
        final DenseStorageReader[] dsr1s = new DenseStorageReader[numInputCols];
        // The arrays are sized to "numInputCols" but only populated up to "numOutputCols".
        // The code in ParseInputToDenseStorge knows that a null DenseStorageWriter means that the
        // column is all-empty and (once the data is confirmed to be empty) just drop the data.
        for (int ii = 0; ii < numOutputCols; ++ii) {
            final DenseStorageWriter dsw = new DenseStorageWriter();
            dsws[ii] = dsw;
            dsr0s[ii] = dsw.newReader();
            dsr1s[ii] = dsw.newReader();
        }

        // Select an Excecutor based on whether the user wants the code to run asynchronously
        // or not.
        final ExecutorService exec =
                specs.concurrent()
                        ? Executors.newFixedThreadPool(numOutputCols + 1)
                        : Executors.newSingleThreadExecutor();

        final ArrayList<Future<ParseDenseStorageToColumn.Result>> sinkFutures = new ArrayList<>();
        try {
            final Future<Long> numRowsFuture =
                    exec.submit(
                            () -> ParseInputToDenseStorage.doit(firstDataRow, specs.nullValueLiteral(), grabber, dsws));

            for (int ii = 0; ii < numOutputCols; ++ii) {
                final List<Parser<?>> parsersToUse = calcParsersToUse(specs, headersToUse[ii], ii + 1);
                final String nullValueLiteralToUse = calcNullValueLiteralToUse(specs, headersToUse[ii], ii + 1);

                final int iiCopy = ii;
                final Future<ParseDenseStorageToColumn.Result> fcb =
                        exec.submit(
                                () -> ParseDenseStorageToColumn.doit(
                                        dsr0s[iiCopy],
                                        dsr1s[iiCopy],
                                        parsersToUse,
                                        specs.nullParser(),
                                        specs.customDoubleParser(),
                                        specs.customTimeZoneParser(),
                                        nullValueLiteralToUse,
                                        sinkFactory));
                sinkFutures.add(fcb);
            }

            final long numRows;
            final ResultColumn[] resultColumns = new ResultColumn[numOutputCols];
            numRows = numRowsFuture.get();
            for (int ii = 0; ii < numOutputCols; ++ii) {
                final ParseDenseStorageToColumn.Result result = sinkFutures.get(ii).get();
                final Object data = result.sink().getUnderlying();
                final DataType dataType = result.dataType();
                resultColumns[ii] = new ResultColumn(headersToUse[ii], data, dataType);
            }
            return new Result(numRows, resultColumns);
        } catch (Exception inner) {
            throw new CsvReaderException("Caught exception", inner);
        } finally {
            // Cancel the sinks (interrupting them if necessary). It is harmless to do this if the sinks
            // have already exited normally.
            for (Future<ParseDenseStorageToColumn.Result> sf : sinkFutures) {
                sf.cancel(true); // Result ignored.
            }
            exec.shutdown();
        }
    }

    /**
     * Determine which list of parsers to use for type inference. Returns {@link CsvSpecs#parsers} unless the user has
     * set an override on a column name or column number basis.
     */
    private static List<Parser<?>> calcParsersToUse(final CsvSpecs specs,
            final String columnName, final int oneBasedColumnNumber) {
        Parser<?> specifiedParser = specs.parserForName().get(columnName);
        if (specifiedParser != null) {
            return Collections.singletonList(specifiedParser);
        }
        specifiedParser = specs.parserForIndex().get(oneBasedColumnNumber);
        if (specifiedParser != null) {
            return Collections.singletonList(specifiedParser);
        }
        return specs.parsers();
    }

    /**
     * Determine which null value literal to use. Returns {@link CsvSpecs#nullValueLiteral()} unless the user has set an
     * override on a column name or column number basis.
     */
    private static String calcNullValueLiteralToUse(final CsvSpecs specs,
            final String columnName, final int oneBasedColumnNumber) {
        String result = specs.nullValueLiteralForName().get(columnName);
        if (result != null) {
            return result;
        }
        result = specs.nullValueLiteralForIndex().get(oneBasedColumnNumber);
        if (result != null) {
            return result;
        }
        return specs.nullValueLiteral();
    }

    /**
     * Determine which headers to use. The result comes from either the first row of the file or the user-specified
     * overrides.
     */
    private static String[] determineHeadersToUse(final CsvSpecs specs,
            final CellGrabber grabber, final MutableObject<byte[][]> firstDataRowHolder)
            throws CsvReaderException {
        String[] headersToUse = null;
        if (specs.hasHeaderRow()) {
            final byte[][] firstRow = tryReadOneRow(grabber);
            if (firstRow == null) {
                throw new CsvReaderException(
                        "Can't proceed because hasHeaders is set but input file is empty");
            }
            headersToUse = Arrays.stream(firstRow).map(String::new).toArray(String[]::new);
        }

        // Whether or not the input had headers, maybe override with client-specified headers.
        if (specs.headers().size() != 0) {
            headersToUse = specs.headers().toArray(new String[0]);
        }

        // If we still have nothing, try generate synthetic column headers (works only if the file is
        // non-empty,
        // because we need to infer the column count).
        final byte[][] firstDataRow;
        if (headersToUse == null) {
            firstDataRow = tryReadOneRow(grabber);
            if (firstDataRow == null) {
                throw new CsvReaderException(
                        "Can't proceed because input file is empty and client has not specified headers");
            }
            headersToUse = new String[firstDataRow.length];
            for (int ii = 0; ii < headersToUse.length; ++ii) {
                headersToUse[ii] = "Column" + (ii + 1);
            }
        } else {
            firstDataRow = null;
        }

        // Apply column specific overrides.
        for (Map.Entry<Integer, String> entry : specs.headerForIndex().entrySet()) {
            headersToUse[entry.getKey() - 1] = entry.getValue();
        }

        firstDataRowHolder.setValue(firstDataRow);
        return headersToUse;
    }

    private static String[] canonicalizeHeaders(CsvSpecs specs, final String[] headers) throws CsvReaderException {
        final String[] legalized = specs.headerLegalizer().apply(headers);
        final Set<String> unique = new HashSet<>();
        final List<String> repeats = new ArrayList<>();
        final List<String> invalidNames = new ArrayList<>();
        for (String header : legalized) {
            if (!unique.add(header)) {
                repeats.add(header);
            } else if (!specs.headerValidator().test(header)) {
                // Using an "else if" because we only want to run each unique name once through the
                // validator.
                invalidNames.add(header);
            }
        }

        if (repeats.isEmpty() && invalidNames.isEmpty()) {
            return legalized;
        }

        final StringBuilder sb = new StringBuilder("Some column headers are invalid.");
        if (!repeats.isEmpty()) {
            sb.append(" Repeated headers: ");
            sb.append(Renderer.renderList(repeats));
        }
        if (!invalidNames.isEmpty()) {
            sb.append(" Invalid headers: ");
            sb.append(Renderer.renderList(invalidNames));
        }
        throw new CsvReaderException(sb.toString());
    }

    /**
     * Try to read one row from the input. Returns false if the input ends before one row has been read.
     *
     * @return The first row as a byte[][] or null if the input was exhausted.
     */
    private static byte[][] tryReadOneRow(final CellGrabber grabber) throws CsvReaderException {
        final List<byte[]> headers = new ArrayList<>();

        // Grab the header
        final ByteSlice slice = new ByteSlice();
        final MutableBoolean lastInRow = new MutableBoolean();
        do {
            if (!grabber.grabNext(slice, lastInRow)) {
                return null;
            }
            final byte[] item = new byte[slice.size()];
            slice.copyTo(item, 0);
            headers.add(item);
        } while (!lastInRow.booleanValue());
        return headers.toArray(new byte[0][]);
    }

    private static byte check7BitAscii(String what, char c) throws CsvReaderException {
        if (c > 0x7f) {
            final String message = String.format("%s is set to '%c' but is required to be 7-bit ASCII",
                    what, c);
            throw new CsvReaderException(message);
        }
        return (byte) c;
    }

    /** Result of {@link #read}. Represents a set of columns. */
    public static final class Result implements Iterable<ResultColumn> {
        private final long numRows;
        private final ResultColumn[] columns;

        public Result(long numRows, ResultColumn[] columns) {
            this.numRows = numRows;
            this.columns = columns;
        }

        /** Number of rows in the input. */
        public long numRows() {
            return numRows;
        }

        /** The number of columns. */
        public int numCols() {
            return columns.length;
        }

        /** The columns. */
        public ResultColumn[] columns() {
            return columns;
        }

        @NotNull
        @Override
        public Iterator<ResultColumn> iterator() {
            return Arrays.stream(columns).iterator();
        }
    }

    /**
     * Represents a column in the {@link Result}.
     */
    public static final class ResultColumn {
        private final String name;
        private final Object data;
        private final DataType dataType;

        public ResultColumn(String name, Object data, DataType dataType) {
            this.name = name;
            this.data = data;
            this.dataType = dataType;
        }

        /** The column name. */
        public String name() {
            return name;
        }

        /**
         * The data for the column. Obtained by invoking {@link Sink#getUnderlying} on the {@link Sink} that built the
         * column, after all processing is done.
         */
        public Object data() {
            return data;
        }

        /**
         * The data type of the column.
         */
        public DataType dataType() {
            return dataType;
        }
    }
}
