package io.deephaven.csv.reading;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.densestorage.DenseStorageReader;
import io.deephaven.csv.parsers.*;
import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.*;

import java.util.*;

import org.jetbrains.annotations.NotNull;

/**
 * The job of this class is to take a column of cell text, as prepared by {@link ParseInputToDenseStorage}, do type
 * inference if appropriate, and parse the text into typed data.
 */
public final class ParseDenseStorageToColumn {
    /**
     * @param colNum The column number being parsed. Some custom sinks use this for their own information.
     * @param ih A freshly-initialized IteratorHolder pointing to the input.
     * @param parsers The set of parsers to try. If null, then {@link Parsers#DEFAULT} will be used.
     * @param specs The CsvSpecs which control how the column is interpreted.
     * @param nullValueLiteralsToUse If a cell text is equal to any of the values in this array, the cell will be
     *        interpreted as the null value. Typically set to a one-element array containing the empty string.
     * @param sinkFactory Factory that makes all the Sinks of various types, used to consume the data we produce.
     * @return The {@link Sink}, provided by the caller's {@link SinkFactory}, that was selected to hold the column
     *         data.
     * @throws CsvReaderException If there is an error processing the input.
     */
    public static Result doit(
            final int colNum,
            final IteratorHolder ih,
            final List<Parser<?>> parsers,
            final CsvSpecs specs,
            final String[] nullValueLiteralsToUse,
            final SinkFactory sinkFactory)
            throws CsvReaderException {
        // Canonicalize the parsers (remove duplicates) but preserve the order.
        Set<Parser<?>> parserSet = new LinkedHashSet<>(parsers != null ? parsers : Parsers.DEFAULT);

        final Tokenizer tokenizer = new Tokenizer(specs.customDoubleParser(), specs.customTimeZoneParser());
        final Parser.GlobalContext gctx =
                new Parser.GlobalContext(colNum, tokenizer, sinkFactory, nullValueLiteralsToUse);

        // Need another IteratorHolder for a second pass through the data.
        final IteratorHolder ihAlt = ih.copyEnsureFresh();

        // Skip over leading null cells. There are four cases:
        // 1. The column is empty. In this case we run the "empty parser"
        // 2. There is only one available parser. In this case we shortcut to that parser and let it deal with the
        // column, whether full of nulls or not. (Doing this early helps certain use cases, such as Deephaven
        // Enterprise, in which column sinks synchronize each other and so it is harmful if one column gets too far
        // ahead without writing to its sink, as would happen in our null-skipping type inference logic).
        // 3. The column is full of all nulls
        // 4. There is a non-null cell (so the type inference process can begin)

        final Parser<?> nullParserToUse =
                parserSet.size() == 1 ? parserSet.iterator().next() : specs.nullParser();

        if (!ih.tryMoveNext()) {
            // Case 1: The column is empty
            if (nullParserToUse == null) {
                throw new CsvReaderException(
                        "Column is empty, so can't infer type of column, and nullParser is not specified.");
            }
            ih.release();
            ihAlt.release();
            return emptyParse(nullParserToUse, gctx);
        }

        if (parserSet.size() == 1) {
            // Case 2. There is only one available parser.
            final Parser<?> parserToUse = parserSet.iterator().next();
            ih.release();
            // Our invariant is that the iterator points to the first element.
            ihAlt.mustMoveNext();
            return OnePhaseParser.onePhaseParse(parserToUse, gctx, ihAlt);
        }

        boolean columnIsAllNulls = true;
        do {
            if (!gctx.isNullCell(ih)) {
                columnIsAllNulls = false;
                break;
            }
        } while (ih.tryMoveNext());

        if (columnIsAllNulls) {
            // case 3. The column is full of all nulls
            if (nullParserToUse == null) {
                throw new CsvReaderException(
                        "Column contains all null cells, so can't infer type of column, and nullParser is not specified.");
            }
            ih.release();
            // Our invariant is that the iterator points to the first element.
            ihAlt.mustMoveNext();
            return OnePhaseParser.onePhaseParse(nullParserToUse, gctx, ihAlt);
        }

        // The rest of this logic is for case 2: there is a non-null cell (so the type inference process can begin).

        final CategorizedParsers cats = CategorizedParsers.create(parserSet);

        // Numerics are special and they get their own fast path that uses Sources and Sinks rather than
        // reparsing the text input.
        final MutableDouble dummyDouble = new MutableDouble();
        if (!cats.numericParsers.isEmpty() && tokenizer.tryParseDouble(ih.bs(), dummyDouble)) {
            return parseNumerics(cats, gctx, ih, ihAlt);
        }

        Parser<?> otherParser = null;
        final MutableBoolean dummyBoolean = new MutableBoolean();
        final MutableLong dummyLong = new MutableLong();
        if (cats.timestampParser != null && tokenizer.tryParseLong(ih.bs(), dummyLong)) {
            otherParser = cats.timestampParser;
        } else if (cats.booleanParser != null && tokenizer.tryParseBoolean(ih.bs(), dummyBoolean)) {
            otherParser = cats.booleanParser;
        } else if (cats.dateTimeParser != null && tokenizer.tryParseDateTime(ih.bs(), dummyLong)) {
            otherParser = cats.dateTimeParser;
        }

        return AFewOtherParsers.parse(cats, otherParser, gctx, ih, ihAlt);
    }

    @NotNull
    private static Result parseNumerics(CategorizedParsers cats, final Parser.GlobalContext gctx,
            IteratorHolder ih, IteratorHolder ihAlt) throws CsvReaderException {
        final List<ParserResultWrapper<?>> wrappers = new ArrayList<>();
        for (Parser<?> parser : cats.numericParsers) {
            final ParserResultWrapper<?> prw = parseNumericsHelper(parser, gctx, ih);
            wrappers.add(prw);
            if (ih.isExhausted()) {
                break;
            }
        }

        if (!ih.isExhausted()) {
            if (cats.customParsers.isEmpty() && cats.charParser == null && cats.stringParser == null) {
                final String message = String.format(
                        "Consumed %d numeric items, then encountered a non-numeric item but there are no custom or char/string parsers available.",
                        ih.numConsumed() - 1);
                throw new CsvReaderException(message);
            }
            // Tried all numeric parsers but couldn't consume all input. Fall back to the char parsers, custom parsers,
            // and string parsers.
            wrappers.clear();
            return AFewOtherParsers.parse(cats, null, gctx, ih, ihAlt);
        }

        ih.release();

        // If all the wrappers implement the Source interface (except possibly the last, which doesn't need to),
        // we can read the data back and cast it to the right numeric type.
        if (canUnify(wrappers)) {
            ihAlt.release();
            return unifyNumericResults(gctx, wrappers);
        }
        // Otherwise (if some wrappers do not implement the Source interface), we have to do a reparse.
        final ParserResultWrapper<?> last = wrappers.get(wrappers.size() - 1);
        return TwoPhaseParser.finishSecondParsePhase(gctx, last, ihAlt);
    }

    private static boolean canUnify(final List<ParserResultWrapper<?>> items) {
        for (int i = 0; i < items.size() - 1; ++i) {
            if (items.get(i).pctx.source() == null) {
                return false;
            }
        }
        return true;
    }

    @NotNull
    private static <TARRAY> ParserResultWrapper<TARRAY> parseNumericsHelper(
            Parser<TARRAY> parser, final Parser.GlobalContext gctx, final IteratorHolder ih)
            throws CsvReaderException {
        final Parser.ParserContext<TARRAY> pctx = parser.makeParserContext(gctx, Parser.CHUNK_SIZE);
        final long begin = ih.numConsumed() - 1;
        final long end = parser.tryParse(gctx, pctx, ih, begin, Long.MAX_VALUE, true);
        return new ParserResultWrapper<>(parser, pctx, begin, end);
    }

    private static class AFewOtherParsers {
        @NotNull
        private static Result parse(
                CategorizedParsers cats,
                final Parser<?> optionalParser,
                final Parser.GlobalContext gctx,
                IteratorHolder ih,
                IteratorHolder ihAlt) throws CsvReaderException {
            List<Parser<?>> inferencingParsers = new ArrayList<>();
            List<Parser<?>> nonInferencingParsers = new ArrayList<>();
            if (optionalParser != null) {
                inferencingParsers.add(optionalParser);
            }
            if (cats.charParser != null) {
                inferencingParsers.add(cats.charParser);
            }

            nonInferencingParsers.addAll(cats.customParsers);
            if (cats.stringParser != null) {
                nonInferencingParsers.add(cats.stringParser);
            }

            Parser<?> lastParser;
            if (!nonInferencingParsers.isEmpty()) {
                lastParser = nonInferencingParsers.remove(nonInferencingParsers.size() - 1);
            } else if (!inferencingParsers.isEmpty()) {
                lastParser = inferencingParsers.remove(inferencingParsers.size() - 1);
            } else {
                throw new CsvReaderException("No available parsers.");
            }

            for (Parser<?> parser : inferencingParsers) {
                final ParserResultWrapper<?> resultWrapper = TwoPhaseParser.tryAdvanceFirstPhase(
                        parser, gctx, ih);

                if (resultWrapper == null) {
                    continue;
                }

                if (resultWrapper.begin == 0) {
                    // Parser completed at input exhaustion, and it started from 0. We are done.
                    return new Result(resultWrapper.pctx.sink(), resultWrapper.pctx.dataType());
                }

                // Parser completed at input exhaustion, but did not start from 0. We need to do the
                // second phase parse (with the same parser) to get all the items in the interval [0..begin).
                // By the assumptions of our algorithm (later parsers accept all inputs of prior parsers),
                // this parse cannot fail.

                ih.release();
                // Our invariant is that the iterator points to the first element.
                ihAlt.mustMoveNext();
                return TwoPhaseParser.finishSecondParsePhase(gctx, resultWrapper, ihAlt);
            }

            // The remaining parsers all work on the input from the beginning. We can let go of the
            // first iterator, because we won't use it again.
            ih.release();

            // Custom parsers do not participate in the two-phase parse algorithm because we know
            // nothing about their structure. So, we give them the entirety of the input.
            for (Parser<?> parser : nonInferencingParsers) {
                final IteratorHolder tempFullIterator = ihAlt.copyEnsureFresh();
                // Our invariant is that the iterator points to the first element.
                tempFullIterator.mustMoveNext(); // Input is not empty, so we know this will succeed.
                final Result result = OnePhaseParser.tryOnePhaseParse(parser, gctx, tempFullIterator);
                if (result != null) {
                    return result;
                }
            }

            // The last parser is special because no parser follows it, so we might as well do
            // a simple one phase parse. If this does not succeed, the whole operation fails.

            // Our invariant is that the iterator points to the first element.
            ihAlt.mustMoveNext();
            return OnePhaseParser.onePhaseParse(lastParser, gctx, ihAlt);
        }
    }

    private static class TwoPhaseParser {
        private static <TARRAY> ParserResultWrapper<TARRAY> tryAdvanceFirstPhase(
                final Parser<TARRAY> parser,
                final Parser.GlobalContext gctx,
                final IteratorHolder ih) throws CsvReaderException {
            final long phaseOneStart = ih.numConsumed() - 1;
            final Parser.ParserContext<TARRAY> pctx = parser.makeParserContext(gctx, Parser.CHUNK_SIZE);
            final long end = parser.tryParse(gctx, pctx, ih, phaseOneStart, Long.MAX_VALUE, true);
            if (!ih.isExhausted()) {
                // This parser couldn't make it to the end but there may be others remaining to try. Signal a
                // failure to the caller so that it can try the next one. Note that 'ih' has been advanced
                // to the failing entry.
                return null;
            }

            return new ParserResultWrapper<>(parser, pctx, phaseOneStart, end);
        }

        private static <TARRAY> Result finishSecondParsePhase(
                final Parser.GlobalContext gctx,
                final ParserResultWrapper<TARRAY> wrapper,
                final IteratorHolder ih) throws CsvReaderException {
            final long end = wrapper.parser.tryParse(gctx, wrapper.pctx, ih, 0, wrapper.begin, false);

            if (end == wrapper.begin) {
                return new Result(wrapper.pctx.sink(), wrapper.pctx.dataType());
            }
            final String message = "Logic error: second parser phase failed on input. Parser was: "
                    + wrapper.parser.getClass().getCanonicalName();
            throw new RuntimeException(message);
        }
    }

    private static class OnePhaseParser {
        @NotNull
        private static <TARRAY> Result onePhaseParse(final Parser<TARRAY> parser, final Parser.GlobalContext gctx,
                final IteratorHolder ih) throws CsvReaderException {
            final Result result = tryOnePhaseParse(parser, gctx, ih);
            if (result != null) {
                return result;
            }
            final String message = String.format(
                    "Parsing failed on input, with nothing left to fall back to. Parser %s successfully parsed %d items before failure.",
                    parser.getClass().getCanonicalName(), ih.numConsumed() - 1);
            throw new CsvReaderException(message);
        }

        private static <TARRAY> Result tryOnePhaseParse(final Parser<TARRAY> parser, final Parser.GlobalContext gctx,
                final IteratorHolder ih) throws CsvReaderException {
            final Parser.ParserContext<TARRAY> pctx = parser.makeParserContext(gctx, Parser.CHUNK_SIZE);
            parser.tryParse(gctx, pctx, ih, 0, Long.MAX_VALUE, true);
            if (ih.isExhausted()) {
                return new Result(pctx.sink(), pctx.dataType());
            }
            return null;
        }
    }

    @NotNull
    private static <TARRAY> Result emptyParse(
            final Parser<TARRAY> parser, final Parser.GlobalContext gctx) throws CsvReaderException {
        // The parser won't do any "parsing" here, but it will create a Sink.
        final Parser.ParserContext<TARRAY> pctx = parser.makeParserContext(gctx, Parser.CHUNK_SIZE);
        parser.tryParse(gctx, pctx, null, 0, 0, true); // Result ignored.
        return new Result(pctx.sink(), pctx.dataType());
    }

    @NotNull
    private static Result unifyNumericResults(
            final Parser.GlobalContext gctx, final List<ParserResultWrapper<?>> wrappers) {
        if (wrappers.isEmpty()) {
            throw new RuntimeException("Logic error: no parser results.");
        }
        final ParserResultWrapper<?> dest = wrappers.get(wrappers.size() - 1);

        // BTW, there's an edge case where there's only one parser in the list. In that case
        // first == dest, but this code still does the right thing.
        final ParserResultWrapper<?> first = wrappers.get(0);
        fillNulls(gctx, dest.pctx, 0, first.begin);

        long destBegin = first.begin;
        for (int ii = 0; ii < wrappers.size() - 1; ++ii) {
            final ParserResultWrapper<?> curr = wrappers.get(ii);
            copy(gctx, curr.pctx, dest.pctx, curr.begin, curr.end, destBegin);
            destBegin += (curr.end - curr.begin);
        }
        return new Result(dest.pctx.sink(), dest.pctx.dataType());
    }

    private static <TARRAY, UARRAY> void copy(
            final Parser.GlobalContext gctx,
            final Parser.ParserContext<TARRAY> sourceCtx,
            final Parser.ParserContext<UARRAY> destCtx,
            final long srcBegin,
            final long srcEnd,
            final long destBegin) {
        TypeConverter.copy(
                sourceCtx.source(),
                destCtx.sink(),
                srcBegin,
                srcEnd,
                destBegin,
                sourceCtx.valueChunk(),
                destCtx.valueChunk(),
                gctx.nullChunk());
    }

    private static <TARRAY> void fillNulls(
            final Parser.GlobalContext gctx,
            final Parser.ParserContext<TARRAY> pctx,
            final long begin,
            final long end) {
        if (begin == end) {
            return;
        }
        final boolean[] nullBuffer = gctx.nullChunk();
        final Sink<TARRAY> destSink = pctx.sink();
        final TARRAY values = pctx.valueChunk();

        final int sizeToInit = Math.min(nullBuffer.length, Math.toIntExact(end - begin));
        Arrays.fill(nullBuffer, 0, sizeToInit, true);

        for (long current = begin; current != end;) { // no ++
            final long endToUse = Math.min(current + nullBuffer.length, end);
            // Don't care about the actual values, only the null flag values (which are all true).
            destSink.write(values, nullBuffer, current, endToUse, false);
            current = endToUse;
        }
    }

    private static <T> List<T> limitToSpecified(Collection<T> items, Set<T> limitTo) {
        final List<T> result = new ArrayList<>();
        for (T item : items) {
            if (limitTo.contains(item)) {
                result.add(item);
            }
        }
        return result;
    }

    /**
     * The result type, containing the Sink that was chosen to hold the data, and the DataType indicating the type of
     * that data.
     */
    public static class Result {
        private final Sink<?> sink;
        private final DataType dataType;

        /**
         * Constructor.
         * 
         * @param sink The Sink that was chosen to hold the data.
         * @param dataType The DataType of the data.
         */
        public Result(Sink<?> sink, DataType dataType) {
            this.sink = sink;
            this.dataType = dataType;
        }

        /**
         * Gets the Sink that was chosen to hold the data
         * 
         * @return The Sink
         */
        public Sink<?> sink() {
            return sink;
        }

        /**
         * Gets the DataType of the data.
         * 
         * @return The DataType of the data.
         */
        public DataType dataType() {
            return dataType;
        }
    }

    private static class CategorizedParsers {
        public static CategorizedParsers create(final Collection<Parser<?>> parsers)
                throws CsvReaderException {
            Parser<?> booleanParser = null;
            final Set<Parser<?>> specifiedNumericParsers = new HashSet<>();
            // Subset of the above.
            final List<Parser<?>> specifiedFloatingPointParsers = new ArrayList<>();
            Parser<?> dateTimeParser = null;
            Parser<?> charParser = null;
            Parser<?> stringParser = null;
            final List<Parser<?>> specifiedTimeStampParsers = new ArrayList<>();
            final List<Parser<?>> specifiedCustomParsers = new ArrayList<>();
            for (Parser<?> p : parsers) {
                if (p == Parsers.BYTE || p == Parsers.SHORT || p == Parsers.INT || p == Parsers.LONG) {
                    specifiedNumericParsers.add(p);
                    continue;
                }

                if (p == Parsers.FLOAT_FAST || p == Parsers.FLOAT_STRICT || p == Parsers.DOUBLE) {
                    specifiedNumericParsers.add(p);
                    specifiedFloatingPointParsers.add(p);
                    continue;
                }

                if (p == Parsers.TIMESTAMP_SECONDS
                        || p == Parsers.TIMESTAMP_MILLIS
                        || p == Parsers.TIMESTAMP_MICROS
                        || p == Parsers.TIMESTAMP_NANOS) {
                    specifiedTimeStampParsers.add(p);
                    continue;
                }

                if (p == Parsers.CHAR) {
                    charParser = p;
                    continue;
                }

                if (p == Parsers.STRING) {
                    stringParser = p;
                    continue;
                }

                if (p == Parsers.BOOLEAN) {
                    booleanParser = p;
                    continue;
                }

                if (p == Parsers.DATETIME) {
                    dateTimeParser = p;
                    continue;
                }

                specifiedCustomParsers.add(p);
            }

            if (specifiedFloatingPointParsers.size() > 1) {
                throw new CsvReaderException(
                        "There is more than one floating point parser in the parser set.");
            }

            if (specifiedTimeStampParsers.size() > 1) {
                throw new CsvReaderException("There is more than one timestamp parser in the parser set.");
            }

            if (!specifiedNumericParsers.isEmpty() && !specifiedTimeStampParsers.isEmpty()) {
                throw new CsvReaderException(
                        "The parser set must not contain both numeric and timestamp parsers.");
            }

            final List<Parser<?>> allNumericParsersByPrecedence =
                    Arrays.asList(
                            Parsers.BYTE,
                            Parsers.SHORT,
                            Parsers.INT,
                            Parsers.LONG,
                            Parsers.FLOAT_FAST,
                            Parsers.FLOAT_STRICT,
                            Parsers.DOUBLE);
            final List<Parser<?>> allCharAndStringParsersByPrecedence =
                    Arrays.asList(Parsers.CHAR, Parsers.STRING);

            final List<Parser<?>> numericParsers =
                    limitToSpecified(allNumericParsersByPrecedence, specifiedNumericParsers);
            final Parser<?> timestampParser =
                    specifiedTimeStampParsers.isEmpty() ? null : specifiedTimeStampParsers.get(0);

            return new CategorizedParsers(
                    booleanParser,
                    numericParsers,
                    dateTimeParser,
                    charParser,
                    stringParser,
                    timestampParser,
                    specifiedCustomParsers);
        }

        private final Parser<?> booleanParser;
        private final List<Parser<?>> numericParsers;
        private final Parser<?> dateTimeParser;
        private final Parser<?> charParser;
        private final Parser<?> stringParser;
        private final Parser<?> timestampParser;
        private final List<Parser<?>> customParsers;

        private CategorizedParsers(
                Parser<?> booleanParser,
                List<Parser<?>> numericParsers,
                Parser<?> dateTimeParser,
                Parser<?> charParser,
                Parser<?> stringParser,
                Parser<?> timestampParser,
                List<Parser<?>> customParsers) {
            this.booleanParser = booleanParser;
            this.numericParsers = numericParsers;
            this.dateTimeParser = dateTimeParser;
            this.charParser = charParser;
            this.stringParser = stringParser;
            this.timestampParser = timestampParser;
            this.customParsers = customParsers;
        }
    }

    private static class ParserResultWrapper<TARRAY> {
        private final Parser<TARRAY> parser;
        private final Parser.ParserContext<TARRAY> pctx;
        private final long begin;
        private final long end;

        public ParserResultWrapper(Parser<TARRAY> parser, Parser.ParserContext<TARRAY> pctx, long begin, long end) {
            this.parser = parser;
            this.pctx = pctx;
            this.begin = begin;
            this.end = end;
        }
    }
}
