package io.deephaven.csv.benchmark.datetimecol;

import io.deephaven.csv.benchmark.util.BenchmarkResult;
import io.deephaven.csv.benchmark.util.DateTimeToLongParser;
import io.deephaven.csv.benchmark.util.TableMaker;
import io.deephaven.csv.benchmark.util.Util;
import org.openjdk.jmh.annotations.*;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(value = 2, jvmArgs = {"-Xms32G", "-Xmx32G"})
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
public class DateTimeColumnBenchmark {
    public static final int ROWS = 1_000_000;
    public static final int COLS = 5;
    public static final int OPERATIONS = ROWS * COLS;

    @State(Scope.Benchmark)
    public static class InputProvider {
        public final TableMaker<long[]> tableMaker;

        public InputProvider() {
            final Random rng = new Random(31337);
            tableMaker = new TableMaker<>(rng, ROWS, COLS, long[]::new, long[][]::new,
                    (r, col, begin, end) -> {
                        while (begin != end) {
                            col[begin++] = Util.make22ndCenturyTimestamp(rng);
                        }
                    },
                    (sb, col, rowIndex) -> {
                        final long seconds = col[rowIndex] / 1_000_000_000;
                        final long nanos = col[rowIndex] % 1_000_000_000;
                        final Instant i = Instant.ofEpochSecond(seconds, nanos);
                        final ZonedDateTime zdt = ZonedDateTime.ofInstant(i, ZoneOffset.UTC);
                        sb.append(zdt);
                    });
        }
    }

    /**
     * For the purpose of benchmarking, we reuse the same storage because we're trying to focus on the cost of parsing,
     * not storage. Also we happen to know the table size beforehand, so we preallocate it. In a real application you
     * would use a growable collection type like TIntArrayList instead.
     */
    @State(Scope.Thread)
    public static class ReusableStorage {
        // We happen to know size of the output. But if not, we could have used a growable collection type instead.
        public final long[][] output = Util.makeArray(ROWS, COLS, long[]::new, long[][]::new);
    }

    BenchmarkResult<long[]> result;

    @TearDown(Level.Invocation)
    public void check(final InputProvider input) {
        input.tableMaker.check(result.columns());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void deephaven(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DateTimeColumnParserDeephaven.read(input.tableMaker.makeStream(), storage.output);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void apache(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DateTimeColumnParserApache.read(input.tableMaker.makeStream(), storage.output,
                new DateTimeToLongParser.Standard());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void apacheDhDtp(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DateTimeColumnParserApache.read(input.tableMaker.makeStream(), storage.output,
                new DateTimeToLongParser.Deephaven());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void fastCsv(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DateTimeColumnParserFastCsv.read(input.tableMaker.makeStream(), storage.output,
                new DateTimeToLongParser.Standard());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void fastCsvDhDtp(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DateTimeColumnParserFastCsv.read(input.tableMaker.makeStream(), storage.output,
                new DateTimeToLongParser.Deephaven());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void jackson(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DateTimeColumnParserJacksonCsv.read(input.tableMaker.makeStream(), input.tableMaker.headers(),
                storage.output, new DateTimeToLongParser.Standard());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void jacksonDhDtp(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DateTimeColumnParserJacksonCsv.read(input.tableMaker.makeStream(), input.tableMaker.headers(),
                storage.output, new DateTimeToLongParser.Deephaven());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void openCsv(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DateTimeColumnParserOpenCsv.read(input.tableMaker.makeStream(), storage.output,
                new DateTimeToLongParser.Standard());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void openCsvDhDtp(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DateTimeColumnParserOpenCsv.read(input.tableMaker.makeStream(), storage.output,
                new DateTimeToLongParser.Deephaven());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void simpleFlatMapper(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        result = DateTimeColumnParserSimpleFlatMapper.read(input.tableMaker.makeStream(), storage.output,
                new DateTimeToLongParser.Standard());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void simpleFlatMapperDhDtp(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        result = DateTimeColumnParserSimpleFlatMapper.read(input.tableMaker.makeStream(), storage.output,
                new DateTimeToLongParser.Deephaven());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void superCsv(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DateTimeColumnParserSuperCsv.read(input.tableMaker.makeStream(), storage.output,
                new DateTimeToLongParser.Standard());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void superCsvDhDtp(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DateTimeColumnParserSuperCsv.read(input.tableMaker.makeStream(), storage.output,
                new DateTimeToLongParser.Deephaven());
    }
}
