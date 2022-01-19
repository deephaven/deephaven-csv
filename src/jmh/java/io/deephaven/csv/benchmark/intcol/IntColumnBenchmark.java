package io.deephaven.csv.benchmark.intcol;

import gnu.trove.list.TByteList;
import gnu.trove.list.array.TByteArrayList;
import io.deephaven.csvbench.MySinkFactory.ResultProvider;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(jvmArgs = {"-Xms2G", "-Xmx2G"})
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
public class IntColumnBenchmark {

    private static final int ROWS = 1000000;
    private static final int COLS = 1;
    private static final int OPERATIONS = ROWS * COLS;

    // TODO: provide better parameterization around this
    @State(Scope.Benchmark)
    public static class CsvBytes {

        private final byte[] input;

        public CsvBytes() {
            final Random random = new Random(31337);
            TByteList bytes = new TByteArrayList();
            bytes.add("X\n".getBytes(StandardCharsets.UTF_8));
            for (int i = 0; i < ROWS; ++i) {
                bytes.add(String.format("%d%n", random.nextInt()).getBytes(StandardCharsets.UTF_8));
            }
            input = bytes.toArray();
        }

        public InputStream inputStream() {
            return new ByteArrayInputStream(input);
        }
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public ResultProvider<int[]> deephaven(CsvBytes bytes) throws Exception {
        return read(bytes, new IntColumnParserDeephaven());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public ResultProvider<int[]> apache(CsvBytes bytes) throws Exception {
        return read(bytes, new IntColumnParserApache());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public ResultProvider<int[]> fastCsv(CsvBytes bytes) throws Exception {
        return read(bytes, new IntColumnParserFastCsv());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public ResultProvider<int[]> jackson(CsvBytes bytes) throws Exception {
        return read(bytes, new IntColumnParserJacksonCsv());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public ResultProvider<int[]> openCsv(CsvBytes bytes) throws Exception {
        return read(bytes, new IntColumnParserOpenCsv());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public ResultProvider<int[]> simpleFlatMapper(CsvBytes bytes) throws Exception {
        return read(bytes, new IntColumnParserSimpleFlatMapper());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public ResultProvider<int[]> superCsv(CsvBytes bytes) throws Exception {
        return read(bytes, new IntColumnParserSuperCsv());
    }

    private static ResultProvider<int[]> read(CsvBytes bytes, IntColumnParser parser) throws Exception {
        return parser.read(bytes.inputStream());
    }
}
