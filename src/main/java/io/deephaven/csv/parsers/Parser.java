package io.deephaven.csv.parsers;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.sinks.Source;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.CsvReaderException;
import java.nio.charset.StandardCharsets;

import org.jetbrains.annotations.NotNull;

/**
 * The Parser interface to the CsvReader. This is implemented by all the built-in parsers {@link IntParser},
 * {@link DoubleParser}, etc, as well as user-defined custom parsers.
 *
 * @param <TARRAY> The underlying type being parsed, represented as an array of that type. e.g. for int, TARRAY would be
 *        int[].
 */
public interface Parser<TARRAY> {
    /**
     * The reusable data chunk size that we use for our parsers.
     */
    int CHUNK_SIZE = 65536 * 4;

    /**
     * Make a context object for the parser. Sample implementation:
     * 
     * <pre>
     * final MySink sink = new MySink();
     * return new ParserContext&lt;&gt;(sink, null, DataType.XXXType, new MyType[chunkSize]);
     * </pre>
     * 
     * <p>
     * Note that parsers other than {Byte,Short,Int,Long}Parser can leave the source field null, as in the above
     * example.
     *
     * @param gctx The GlobalContext. Built-in parsers use this to access the SinkFactory so that they can make a Sink
     *        of the right type. Custom parsers will probably not need this.
     * @param chunkSize The size of the chunk to create.
     * @return The ParserContext.
     */
    @NotNull
    ParserContext<TARRAY> makeParserContext(final GlobalContext gctx, final int chunkSize);

    /**
     * Tries to parse the data pointed to by IteratorHolder 'ih' into a Sink. The method parses as many values as it
     * can. It stops when:
     *
     * <ol>
     * <li>The range [{@code destBegin},{@code destEnd}) is full, or
     * <li>The iterator {@code ih} is exhausted, or
     * <li>The code encounters a source value that it is unable to parse.
     * </ol>
     *
     * @param gctx The {@link GlobalContext} holding various shared parameters for the parse. This will be shared among
     *        parsers of different types as the type inference process proceeds.
     * @param pctx The {@link ParserContext} for this specific parser. It will be the object created by the call to
     *        {Parser#makeContext}. If the caller calls {@link Parser#tryParse} multiple times (for example during
     *        two-phase parsing), it will pass the same {@link ParserContext} object each time.
     * @param ih An IteratorHolder pointing to the data. It is already pointing to the current element or the end (in
     *        other words, it has had {@link IteratorHolder#tryMoveNext} called on it at least once). The reason for
     *        this invariant is because other code (controlling logic and other parsers) have needed to peek at the
     *        current element before getting here in order to decide what to do.
     * @param begin The start of the range (inclusive) to write values to.
     * @param end The end of the range (exclusive) to write values to. This can also be a very large value like
     *        Long.MAX_VALUE if the caller does not know how many values there are.
     * @param appending Whether the parser is being called in a mode where it is appending to the end of the
     *        {@link Sink} or replacing previously-written pad values in the {@link Sink}. This value is simply passed
     *        on to {@link Sink#write} which may use it as a hint to slightly simplify its logic.
     * @return The end range (exclusive) of the values parsed. Returns {@code begin} if no values were parsed.
     * @throws CsvReaderException if the parse is not successful
     */
    long tryParse(
            GlobalContext gctx,
            ParserContext<TARRAY> pctx,
            IteratorHolder ih,
            long begin,
            long end,
            boolean appending)
            throws CsvReaderException;

    /**
     * Per-parser context used for parsing state as the processing progresses. Each Parser has access to one of these,
     * plus a type-specific ParseContext that it creates itself.
     */
    class GlobalContext {
        /**
         * The 0-based column number that the parser is working on.
         */
        private final int colNum;
        /**
         * The Tokenizer is responsible for parsing entities like ints, doubles, supported DateTime formats, etc.
         */
        private final Tokenizer tokenizer;
        /** Caller-specified interface for making all the various Sink&lt;TARRAY&gt; types. */
        private final SinkFactory sinkFactory;
        /**
         * Whether all the cells seen so far are the "null" indicator (usually the empty string), or are 1 character in
         * length. This is used when inferring char vs String.
         */
        private boolean isNullOrWidthOneSoFar;
        /**
         * The array of null sentinels, each encoded in UTF-8. The user can configure as many null sentinels as they
         * want (including no null sentinels).
         */
        private final byte[][] nullSentinelsAsBytes;
        /** An "isNull" chunk */
        private final boolean[] nullChunk;

        /**
         * Constructor.
         * 
         * @param colNum The column number we are working with
         * @param tokenizer A Tokenizer to help with tokenizing the input
         * @param sinkFactory Caller-specified interface for making the various Sink objects
         * @param nullValueLiterals The null value literals appropriate for this column.
         */
        public GlobalContext(final int colNum, final Tokenizer tokenizer, final SinkFactory sinkFactory,
                final String[] nullValueLiterals) {
            this.colNum = colNum;
            this.tokenizer = tokenizer;
            this.sinkFactory = sinkFactory;
            isNullOrWidthOneSoFar = true;

            // Process the nullValueLiterals into UTF-8 byte arrays so the isNullCell test can run more efficiently.
            nullSentinelsAsBytes = new byte[nullValueLiterals.length][];
            for (int ii = 0; ii < nullValueLiterals.length; ++ii) {
                nullSentinelsAsBytes[ii] = nullValueLiterals[ii].getBytes(StandardCharsets.UTF_8);
            }
            nullChunk = new boolean[CHUNK_SIZE];
        }

        /**
         * Determines whether the iterator's current text contains the null value literal. The notion of "null value
         * literal" is user-configurable on a per-column basis, but is typically the empty string.
         *
         * @param ih The IteratorHolder
         * @return whether the iterator's current text contains the null cell.
         */
        public boolean isNullCell(final IteratorHolder ih) {
            for (final byte[] nullSentinel : nullSentinelsAsBytes) {
                if (equals(
                        ih.bs().data(),
                        ih.bs().begin(),
                        ih.bs().end(),
                        nullSentinel,
                        0,
                        nullSentinel.length)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Gets the column number
         * 
         * @return The Column number
         */
        public int colNum() {
            return colNum;
        }

        /**
         * Gets the Tokenizer
         * 
         * @return The Tokenizer
         */
        public Tokenizer tokenizer() {
            return tokenizer;
        }

        /**
         * Gets the SinkFactory
         * 
         * @return The SinkFactory
         */
        public SinkFactory sinkFactory() {
            return sinkFactory;
        }

        /**
         * Gets the isNullOrWidthOneSoFar flag
         * 
         * @return The isNullOrWidthOneSoFar flag
         */
        public boolean isNullOrWidthOneSoFar() {
            return isNullOrWidthOneSoFar;
        }

        /**
         * Clear the "isNullOrWidthOneSoFar" flag.
         */
        public void clearIsNullOrWidthOneSoFar() {
            isNullOrWidthOneSoFar = false;
        }

        /**
         * Gets the set (an array) of null sentinels, each of which is a byte array of UTF-8 bytes.
         * 
         * @return The set (an array) of null sentinels
         */
        public byte[][] nullSentinelsAsBytes() {
            return nullSentinelsAsBytes;
        }

        /**
         * Gets the reusable nullChunk data structure.
         * 
         * @return The reusable nullChunk data structure.
         */
        public boolean[] nullChunk() {
            return nullChunk;
        }

        // If bumping language level up to 11, can replace with Arrays.equals()
        private static boolean equals(byte[] a, int aFromIndex, int aToIndex, byte[] b, int bFromIndex, int bToIndex) {
            int aLength = aToIndex - aFromIndex;
            int bLength = bToIndex - bFromIndex;
            if (aLength != bLength) {
                return false;
            }
            for (int i = 0; i < aLength; ++i) {
                if (a[aFromIndex + i] != b[bFromIndex + i]) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * This is per-parser context which is both created and consumed by individual parsers. It is basically the local
     * state of the Parser. This is needed because our internal parsers are all stateless (each of them has a singleton
     * INSTANCE variable). They use this class to store their state.
     * 
     * @param <TARRAY> The underlying array type of the parser.
     */
    class ParserContext<TARRAY> {
        private final Sink<TARRAY> sink;
        private final Source<TARRAY> source;
        private final DataType dataType;
        private final TARRAY valueChunk;

        /**
         * Constructor
         * 
         * @param sink The Parser's Sink object
         * @param source The Parser's Source object
         * @param dataType The Parser's DataType
         * @param valueChunk The reusable value chunk for the Parser to store chunks of data.
         */
        public ParserContext(final Sink<TARRAY> sink, final Source<TARRAY> source,
                final DataType dataType, final TARRAY valueChunk) {
            this.sink = sink;
            this.source = source;
            this.dataType = dataType;
            this.valueChunk = valueChunk;
        }

        /**
         * Gets the Sink object
         * 
         * @return The Sink object
         */
        public Sink<TARRAY> sink() {
            return sink;
        }

        /**
         * Gets the Source object
         * 
         * @return The Source object
         */
        public Source<TARRAY> source() {
            return source;
        }

        /**
         * Gets the DataType
         * 
         * @return The DataType
         */
        public DataType dataType() {
            return dataType;
        }

        /**
         * Gets the reusable data chunk object
         * 
         * @return The reusable data chunk object
         */
        public TARRAY valueChunk() {
            return valueChunk;
        }
    }
}
