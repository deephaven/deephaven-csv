package io.deephaven.csv.benchmark.stringcol;

import io.deephaven.csv.benchmark.util.BenchmarkResult;
import io.deephaven.csv.benchmark.util.TableMaker;
import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Fork(value = 2, jvmArgs = {"-Xms32G", "-Xmx32G"})
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
public class StringColumnBenchmark {
    public static final int ROWS = 1_000_000;
    public static final int COLS = 5;
    public static final int OPERATIONS = ROWS * COLS;

    private static final int STRING_LENGTH = 25;

    @State(Scope.Benchmark)
    public static class InputProvider {
        public final TableMaker<String[]> tableMaker;

        public InputProvider() {
            final char[] chars = new char[STRING_LENGTH];
            final Random rng = new Random(31337);

            final Function<Random, String> makeString = r -> {
                for (int charIndex = 0; charIndex < STRING_LENGTH; ++charIndex) {
                    chars[charIndex] = (char) ('a' + rng.nextInt(26));
                }
                return new String(chars);
            };

            tableMaker = new TableMaker<>(rng, ROWS, COLS, String[]::new, String[][]::new,
                    (r, col, begin, end) -> {
                        while (begin != end) {
                            col[begin++] = makeString.apply(r);
                        }
                    },
                    (sb, col, rowIndex) -> sb.append(col[rowIndex]));
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
        private final String[][] output;

        public ReusableStorage() {
            output = new String[COLS][];
            for (int col = 0; col < COLS; ++col) {
                output[col] = new String[ROWS];
            }
        }

        public String[][] output() {
            return output;
        }
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<String[]> deephaven(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        return StringColumnParserDeephaven.read(input.tableMaker.makeStream(), storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<String[]> apache(final InputProvider input, final ReusableStorage storage) throws Exception {
        return StringColumnParserApache.read(input.tableMaker.makeStream(), storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<String[]> fastCsv(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        return StringColumnParserFastCsv.read(input.tableMaker.makeStream(), storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<String[]> jackson(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        return StringColumnParserJacksonCsv.read(input.tableMaker.makeStream(), input.tableMaker.headers(),
                storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<String[]> openCsv(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        return StringColumnParserOpenCsv.read(input.tableMaker.makeStream(), storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<String[]> simpleFlatMapper(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        return StringColumnParserSimpleFlatMapper.read(input.tableMaker.makeStream(), storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public BenchmarkResult<String[]> superCsv(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        return StringColumnParserSuperCsv.read(input.tableMaker.makeStream(), storage.output());
    }
}
