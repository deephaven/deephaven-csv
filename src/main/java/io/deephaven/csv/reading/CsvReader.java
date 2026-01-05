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
     * @throws CsvReaderException if an error occurred while reading the input
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
        return commonReadLogic(specs, grabber, firstDataRow, numInputCols, numOutputCols, headersTemp2, sinkFactory);
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
            String[] headersBeforeLegalization, final SinkFactory sinkFactory)
            throws CsvReaderException {

        final String[][] nullValueLiteralsToUse = new String[numOutputCols][];
        for (int ii = 0; ii < numOutputCols; ++ii) {
            nullValueLiteralsToUse[ii] =
                    calcNullValueLiteralsToUse(specs, headersBeforeLegalization[ii], ii).toArray(new String[0]);
        }

        final String[] headersToUse = canonicalizeHeaders(specs, headersBeforeLegalization);

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

        // Create a lambda for the writer
        final Callable<Long> writerLambda = () -> ParseInputToDenseStorage.doit(headersToUse,
                optionalFirstDataRow, grabber, specs, nullValueLiteralsToUse, dsws);

        // Create lambdas for the readers, taking care to not hold a reference to the DenseStorageReader.
        // This is important in the concurrent case so the garbage collector can clean up queue blocks that no
        // reader is looking at anymore.
        final ArrayList<Callable<ParseDenseStorageToColumn.Result>> readerLambdas = new ArrayList<>();
        for (int ii = 0; ii < numOutputCols; ++ii) {
            final List<Parser<?>> parsersToUse = calcParsersToUse(specs, headersBeforeLegalization[ii], ii);

            final int iiCopy = ii;
            final Moveable<DenseStorageReader> movedDsr = dsrs.get(ii).move();
            final String[] nvls = nullValueLiteralsToUse[ii];

            readerLambdas.add(() -> ParseDenseStorageToColumn.doit(
                    iiCopy, // 0-based column numbers
                    movedDsr,
                    parsersToUse,
                    specs,
                    nvls,
                    sinkFactory));
        }

        try {
            // The writer result.
            long numRows = 0;
            // The reader results.
            final ArrayList<ParseDenseStorageToColumn.Result> readerResults = new ArrayList<>();

            // If the user wants concurrency, create an executor to run the above lambdas concurrently. Otherwise,
            // the above lambdas will be run sequentially.
            if (specs.concurrent()) {
                final ExecutorService executorService = Executors.newFixedThreadPool(numOutputCols + 1);
                // Our CompletionService unfortunately has type Object because of the diversity of lambdas
                // we submit to it (the writer has type Long and the readers have type ParseDenseStorageToColumn.Result)
                final CompletionService<Object> ecs = new ExecutorCompletionService<>(executorService);
                // Do some work to catch the inner exception, if there is one.
                Throwable innerThrowable = null;
                try {
                    final Future<Object> writerFuture = ecs.submit(writerLambda::call);

                    final ArrayList<Future<Object>> readerFutures = new ArrayList<>();
                    for (Callable<ParseDenseStorageToColumn.Result> readerLambda : readerLambdas) {
                        readerFutures.add(ecs.submit(readerLambda::call));
                    }

                    // Observe (but ignore) the results as they asynchronously come back from the CompletionService.
                    // In this way, anyone that throws an exception rethrows here, which will cause our finally block
                    // to shut down the CompletionService, which will in turn shut down still-running work.
                    // +1 because we have submitted one writer as well as all the readers.
                    for (int ii = 0; ii < readerLambdas.size() + 1; ++ii) {
                        Object ignored = ecs.take().get();
                    }

                    // If we get here, all the futures have completed successfully. Pull out their values.
                    numRows = (long) writerFuture.get();
                    for (Future<Object> readerFuture : readerFutures) {
                        readerResults.add((ParseDenseStorageToColumn.Result) readerFuture.get());
                    }
                } catch (ExecutionException e) {
                    innerThrowable = e.getCause();
                } catch (Throwable t) {
                    innerThrowable = t;
                } finally {
                    executorService.shutdownNow();
                    final boolean allTerminated =
                            executorService.awaitTermination(specs.threadShutdownTimeout().toNanos(),
                                    TimeUnit.NANOSECONDS);
                    if (!allTerminated) {
                        final String message = String.format(
                                "Failed to shutdown all threads (after waiting %s)",
                                specs.threadShutdownTimeout());
                        if (innerThrowable == null) {
                            // We probably can't get here, since we wait for all the futures in the try block above.
                            throw new TimeoutException(message);
                        }
                        innerThrowable.addSuppressed(new TimeoutException(message));
                    }
                    if (innerThrowable != null) {
                        throw innerThrowable;
                    }
                }
            } else {
                // Sequentially run the writer, then each reader.
                numRows = writerLambda.call();
                for (Callable<ParseDenseStorageToColumn.Result> readerLambda : readerLambdas) {
                    readerResults.add(readerLambda.call());
                }
            }

            final ResultColumn[] resultColumns = new ResultColumn[numOutputCols];
            for (int ii = 0; ii < numOutputCols; ++ii) {
                final ParseDenseStorageToColumn.Result result = readerResults.get(ii);
                final Object data = result.sink().getUnderlying();
                final DataType dataType = result.dataType();
                resultColumns[ii] = new ResultColumn(headersToUse[ii], data, dataType);
            }
            return new Result(numRows, resultColumns);
        } catch (Throwable throwable) {
            throw new CsvReaderException("Caught exception", throwable);
        }
    }

    /**
     * Determine which list of parsers to use for type inference. Returns {@link CsvSpecs#parsers} unless the user has
     * set an override on a column name or column number basis.
     */
    private static List<Parser<?>> calcParsersToUse(final CsvSpecs specs, final String columnName,
            final int columnIndex) {
        List<Parser<?>> specifiedParsers = specs.parsersForName().get(columnName);
        if (specifiedParsers != null) {
            return specifiedParsers;
        }
        specifiedParsers = specs.parsersForIndex().get(columnIndex);
        if (specifiedParsers != null) {
            return specifiedParsers;
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
        // legalizer is allowed to mutate the input in-place, so we clone it before passing it.
        final String[] legalized = specs.headerLegalizer().apply(headers.clone());
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

        /**
         * Constructor.
         * 
         * @param numRows Number of rows in the result.
         * @param columns The columns comprising the result.
         */
        public Result(long numRows, ResultColumn[] columns) {
            this.numRows = numRows;
            this.columns = columns;
        }

        /**
         * Number of rows in the input.
         * 
         * @return The number of rows in the input
         */
        public long numRows() {
            return numRows;
        }

        /**
         * The number of columns.
         * 
         * @return The number of columns in the input
         */
        public int numCols() {
            return columns.length;
        }

        /**
         * The columns.
         * 
         * @return The columns of the input, parses as an array of ResultColumn
         */
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

        /**
         * Constructor.
         * 
         * @param name The name of the column.
         * @param data The column data.
         * @param dataType The type of the column.
         */
        public ResultColumn(String name, Object data, DataType dataType) {
            this.name = name;
            this.data = data;
            this.dataType = dataType;
        }

        /**
         * The column name.
         * 
         * @return The name
         */
        public String name() {
            return name;
        }

        /**
         * The data for the column. Obtained by invoking {@link Sink#getUnderlying} on the {@link Sink} that built the
         * column, after all processing is done.
         * 
         * @return The data.
         */
        public Object data() {
            return data;
        }

        /**
         * The data type of the column.
         * 
         * @return The data type.
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
