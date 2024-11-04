package io.deephaven.csv.reading;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.densestorage.DenseStorageReader;
import io.deephaven.csv.densestorage.DenseStorageWriter;
import io.deephaven.csv.parsers.DataType;
import io.deephaven.csv.parsers.Parser;
import io.deephaven.csv.reading.cells.CellGrabber;
import io.deephaven.csv.reading.cells.DelimitedCellGrabber;
import io.deephaven.csv.reading.cells.FixedCellGrabber;
import io.deephaven.csv.reading.headers.DelimitedHeaderFinder;
import io.deephaven.csv.reading.headers.FixedHeaderFinder;
import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.util.*;
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
     *        in the CsvReader.Result. Thread safety: The {@link SinkFactory} may be invoked concurrently, therefore it
     *        must be thread safe.
     * @return A CsvReader.Result containing the column names, the number of columns, and the final set of
     *         fully-populated Sinks.
     */
    public static Result read(final CsvSpecs specs, final InputStream stream, final SinkFactory sinkFactory)
            throws CsvReaderException {
        return specs.hasFixedWidthColumns() ? fixedReadLogic(specs, stream, sinkFactory)
                : delimitedReadLogic(specs, stream, sinkFactory);
    }

    private static Result delimitedReadLogic(
            final CsvSpecs specs, final InputStream stream, final SinkFactory sinkFactory)
            throws CsvReaderException {
        // These two have already been validated by CsvSpecs to be 7-bit ASCII.
        final byte quoteAsByte = (byte) specs.quote();
        final byte delimiterAsByte = (byte) specs.delimiter();
        final CellGrabber grabber =
                new DelimitedCellGrabber(stream, quoteAsByte, delimiterAsByte, specs.ignoreSurroundingSpaces(),
                        specs.trim());
        // For an "out" parameter
        final MutableObject<byte[][]> firstDataRowHolder = new MutableObject<>();
        final String[] headersTemp = DelimitedHeaderFinder.determineHeadersToUse(specs, grabber,
                firstDataRowHolder);
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

        return commonReadLogic(specs, grabber, firstDataRow, numInputCols, numOutputCols, headersToUse, sinkFactory);
    }

    private static Result fixedReadLogic(
            final CsvSpecs specs, final InputStream stream, final SinkFactory sinkFactory) throws CsvReaderException {
        final CellGrabber lineGrabber = FixedCellGrabber.makeLineGrabber(stream);
        MutableObject<int[]> columnWidths = new MutableObject<>();
        final String[] headers = FixedHeaderFinder.determineHeadersToUse(specs, lineGrabber, columnWidths);
        final int numCols = headers.length;
        final CellGrabber grabber = new FixedCellGrabber(lineGrabber, columnWidths.getValue(),
                specs.ignoreSurroundingSpaces(), specs.useUtf32CountingConvention());
        return commonReadLogic(specs, grabber, null, numCols, numCols, headers, sinkFactory);
    }

    private static Result commonReadLogic(final CsvSpecs specs, CellGrabber grabber, byte[][] optionalFirstDataRow,
            int numInputCols, int numOutputCols,
            String[] headersToUse, final SinkFactory sinkFactory)
            throws CsvReaderException {
        final String[][] nullValueLiteralsToUse = new String[numOutputCols][];
        for (int ii = 0; ii < numOutputCols; ++ii) {
            nullValueLiteralsToUse[ii] =
                    calcNullValueLiteralsToUse(specs, headersToUse[ii], ii).toArray(new String[0]);
        }

        // Create a DenseStorageWriter for each column. The arrays are sized to "numInputCols" but only populated up to
        // "numOutputCols". The remaining (numInputCols - numOutputCols) are set to null. The code in
        // parseInputToDenseStorge knows that having a null DenseStorageWriter means that the column is all-empty and
        // (once the data is confirmed to be empty) just drops the data. "While we're here" we also make the List
        // (not array, because Java generics) of DenseStorageReaders. This list is of size numOutputCols and is used
        // down below to hand to each parseDenseStorageToColumn reader in a separate thread.
        final DenseStorageWriter[] dsws = new DenseStorageWriter[numInputCols];
        final List<Moveable<DenseStorageReader>> dsrs = new ArrayList<>();
        for (int ii = 0; ii < numOutputCols; ++ii) {
            final Pair<DenseStorageWriter, DenseStorageReader> pair = DenseStorageWriter.create(specs.concurrent());
            dsws[ii] = pair.first;
            dsrs.add(new Moveable<>(pair.second));
        }

        // Select an Excecutor based on whether the user wants the code to run asynchronously or not.
        final Executor exec;
        final ExecutorService executorService;
        if (specs.concurrent()) {
            exec = executorService = Executors.newFixedThreadPool(numOutputCols + 1);
        } else {
            exec = DirectExecutor.INSTANCE;
            executorService = null;
        }
        // We are generic on Object because we have a diversity of Future types (Long vs
        // ParseDenseStorageToColumn.Result)
        final ExecutorCompletionService<Object> ecs = new ExecutorCompletionService<>(exec);

        // Start the writer.
        final Future<Object> numRowsFuture = ecs.submit(() -> ParseInputToDenseStorage.doit(headersToUse,
                optionalFirstDataRow, grabber, specs, nullValueLiteralsToUse, dsws));

        // Start the readers, taking care to not hold a reference to the DenseStorageReader.
        final ArrayList<Future<Object>> sinkFutures = new ArrayList<>();
        try {
            for (int ii = 0; ii < numOutputCols; ++ii) {
                final List<Parser<?>> parsersToUse = calcParsersToUse(specs, headersToUse[ii], ii);

                final int iiCopy = ii;
                final Future<Object> fcb = ecs.submit(
                        () -> ParseDenseStorageToColumn.doit(
                                iiCopy, // 0-based column numbers
                                dsrs.get(iiCopy).move(),
                                parsersToUse,
                                specs,
                                nullValueLiteralsToUse[iiCopy],
                                sinkFactory));
                sinkFutures.add(fcb);
            }

            // Get each task as it finishes. If a task finishes with an exception, we will throw here.
            for (int ii = 0; ii < numOutputCols + 1; ++ii) {
                ecs.take().get();
            }

            final long numRows = (long) numRowsFuture.get();
            final ResultColumn[] resultColumns = new ResultColumn[numOutputCols];
            for (int ii = 0; ii < numOutputCols; ++ii) {
                final ParseDenseStorageToColumn.Result result =
                        (ParseDenseStorageToColumn.Result) sinkFutures.get(ii).get();
                final Object data = result.sink().getUnderlying();
                final DataType dataType = result.dataType();
                resultColumns[ii] = new ResultColumn(headersToUse[ii], data, dataType);
            }
            return new Result(numRows, resultColumns);
        } catch (Throwable throwable) {
            throw new CsvReaderException("Caught exception", throwable);
        } finally {
            if (executorService != null) {
                // Tear down everything (interrupting the threads if necessary).
                executorService.shutdownNow();
            }
        }
    }

    /**
     * Determine which list of parsers to use for type inference. Returns {@link CsvSpecs#parsers} unless the user has
     * set an override on a column name or column number basis.
     */
    private static List<Parser<?>> calcParsersToUse(final CsvSpecs specs, final String columnName,
            final int columnIndex) {
        Parser<?> specifiedParser = specs.parserForName().get(columnName);
        if (specifiedParser != null) {
            return Collections.singletonList(specifiedParser);
        }
        specifiedParser = specs.parserForIndex().get(columnIndex);
        if (specifiedParser != null) {
            return Collections.singletonList(specifiedParser);
        }
        return specs.parsers();
    }

    /**
     * Determine which null value literal to use. Returns {@link CsvSpecs#nullValueLiterals()} unless the user has set
     * an override on a column name or column number basis.
     */
    private static List<String> calcNullValueLiteralsToUse(final CsvSpecs specs, final String columnName,
            final int columnIndex) {
        List<String> result = specs.nullValueLiteralsForName().get(columnName);
        if (result != null) {
            return result;
        }
        result = specs.nullValueLiteralsForIndex().get(columnIndex);
        if (result != null) {
            return result;
        }
        return specs.nullValueLiterals();
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

    private enum DirectExecutor implements Executor {
        INSTANCE;

        @Override
        public void execute(@NotNull Runnable command) {
            command.run();
        }
    }
}
