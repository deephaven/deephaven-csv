package io.deephaven.csv.benchmark.datetimecol;

import io.deephaven.csv.benchmark.util.BenchmarkResult;
import io.deephaven.csv.benchmark.util.DateTimeToLongParser.Deephaven;
import io.deephaven.csv.benchmark.util.DateTimeToLongParser.Standard;
import io.deephaven.csv.benchmark.util.TableMaker;
import io.deephaven.csv.benchmark.util.Util;
import org.openjdk.jmh.annotations.*;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
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

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<long[]> deephaven(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        return DateTimeColumnParserDeephaven.read(input.tableMaker.makeStream(), storage.output);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<long[]> apache(final InputProvider input, final ReusableStorage storage) throws Exception {
        return DateTimeColumnParserApache.read(input.tableMaker.makeStream(), storage.output, Standard.INSTANCE);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<long[]> apacheDhDtp(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        return DateTimeColumnParserApache.read(input.tableMaker.makeStream(), storage.output, new Deephaven());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<long[]> fastCsv(final InputProvider input, final ReusableStorage storage) throws Exception {
        return DateTimeColumnParserFastCsv.read(input.tableMaker.makeStream(), storage.output, Standard.INSTANCE);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<long[]> fastCsvDhDtp(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        return DateTimeColumnParserFastCsv.read(input.tableMaker.makeStream(), storage.output, new Deephaven());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<long[]> jackson(final InputProvider input, final ReusableStorage storage) throws Exception {
        return DateTimeColumnParserJacksonCsv.read(input.tableMaker.makeStream(), input.tableMaker.headers(),
                storage.output, Standard.INSTANCE);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<long[]> jacksonDhDtp(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        return DateTimeColumnParserJacksonCsv.read(input.tableMaker.makeStream(), input.tableMaker.headers(),
                storage.output, new Deephaven());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<long[]> openCsv(final InputProvider input, final ReusableStorage storage) throws Exception {
        return DateTimeColumnParserOpenCsv.read(input.tableMaker.makeStream(), storage.output, Standard.INSTANCE);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<long[]> openCsvDhDtp(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        return DateTimeColumnParserOpenCsv.read(input.tableMaker.makeStream(), storage.output, new Deephaven());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<long[]> simpleFlatMapper(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        return DateTimeColumnParserSimpleFlatMapper.read(input.tableMaker.makeStream(), storage.output,
                Standard.INSTANCE);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<long[]> simpleFlatMapperDhDtp(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        return DateTimeColumnParserSimpleFlatMapper.read(input.tableMaker.makeStream(), storage.output,
                new Deephaven());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<long[]> superCsv(final InputProvider input, final ReusableStorage storage) throws Exception {
        return DateTimeColumnParserSuperCsv.read(input.tableMaker.makeStream(), storage.output, Standard.INSTANCE);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<long[]> superCsvDhDtp(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        return DateTimeColumnParserSuperCsv.read(input.tableMaker.makeStream(), storage.output, new Deephaven());
    }
}
