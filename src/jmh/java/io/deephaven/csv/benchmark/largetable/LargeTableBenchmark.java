package io.deephaven.csv.benchmark.largetable;

import io.deephaven.csv.benchmark.doublecol.DoubleColumnBenchmark;
import io.deephaven.csv.benchmark.util.Util;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(value = 1, jvmArgs = {"-Xms32G", "-Xmx32G"})
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
public class LargeTableBenchmark {
    /**
     * Name of the test file.
     */
    private static final String LARGE_FILE_PATH = "/mnt/ramdisk/largefile.csv";
    /**
     * Number of rows in the test CSV file
     */
    private static final int ROWS = 100_000_000;
    /**
     * Amount to buffer in memory before writing out to file.
     */
    private static final int SB_FLUSH_THRESHOLD = 10_000_000;
    /**
     * Size of randomly-generated strings.
     */
    private static final int STRING_LENGTH = 25;

    @State(Scope.Thread)
    public static class InputProvider {
        public final String[] headers;

        public InputProvider() {
            this.headers = new String[] {
                    "Timestamp", "String", "Bool", "Long0", "Long1", "Double0", "Double1", "Double2"
            };

            try {
                if (Files.exists(Paths.get(LARGE_FILE_PATH))) {
                    System.out.println(LARGE_FILE_PATH + " already exists. Reusing file.");
                } else {
                    System.out.println(LARGE_FILE_PATH + " does not exist. Creating new file.");
                    makeLargeFile(LARGE_FILE_PATH);
                }
            } catch (Exception e) {
                throw new RuntimeException("Caught exception", e);
            }
        }

        public InputStream makeStream() throws Exception {
            return new FileInputStream(LARGE_FILE_PATH);
        }

        private void makeLargeFile(final String fileName) throws Exception {
            final Random rng = new Random(12345);
            final StringBuilder sb = new StringBuilder();
            try (final FileOutputStream fos = new FileOutputStream(fileName)) {
                final String headerText = String.join(",", headers) + '\n';
                fos.write(headerText.getBytes(StandardCharsets.UTF_8));

                for (int ii = 0; ii < ROWS; ++ii) {
                    appendTimestamp(sb, rng);
                    sb.append(',');
                    appendString(sb, rng);
                    sb.append(',');
                    appendBool(sb, rng);
                    sb.append(',');
                    appendLong(sb, rng);
                    sb.append(',');
                    appendLong(sb, rng);
                    sb.append(',');
                    appendDouble(sb, rng);
                    sb.append(',');
                    appendDouble(sb, rng);
                    sb.append(',');
                    appendDouble(sb, rng);
                    sb.append('\n');
                    if (sb.length() >= SB_FLUSH_THRESHOLD) {
                        fos.write(sb.toString().getBytes(StandardCharsets.UTF_8));
                        sb.setLength(0);
                    }
                    if ((ii % 1_000_000) == 0) {
                        System.out.printf("%d out of %d written (%d%%)\n",
                                ii, ROWS, ((long) ii * 100) / ROWS);
                    }
                }
                // Flush any residual
                fos.write(sb.toString().getBytes(StandardCharsets.UTF_8));
                System.out.println("Done writing file.");
            }
        }

        private static void appendTimestamp(final StringBuilder sb, final Random rng) {
            final long ts = Util.make22ndCenturyTimestamp(rng);
            final long seconds = ts / 1_000_000_000;
            final long nanos = ts % 1_000_000_000;
            final Instant i = Instant.ofEpochSecond(seconds, nanos);
            final ZonedDateTime zdt = ZonedDateTime.ofInstant(i, ZoneOffset.UTC);
            sb.append(zdt);
        }

        private static void appendString(final StringBuilder sb, final Random rng) {
            for (int ii = 0; ii < STRING_LENGTH; ++ii) {
                final char c = (char) (rng.nextInt(26) + 'a');
                sb.append(c);
            }
        }

        private static void appendBool(final StringBuilder sb, final Random rng) {
            sb.append(rng.nextBoolean() ? "true" : "false");
        }

        private static void appendLong(final StringBuilder sb, final Random rng) {
            sb.append(rng.nextLong());
        }

        private static void appendDouble(final StringBuilder sb, final Random rng) {
            sb.append(rng.nextDouble());
        }
    }

    /**
     * For the purpose of benchmarking, we reuse the same storage because we're trying to focus on the cost of parsing,
     * not allocating storage.
     */
    @State(Scope.Thread)
    public static class ReusableStorage {
        // We happen to know the size of the output beforehand. Otherwise we would have used a growable collection.
        public final Results results = new Results(ROWS);
    }

    @Benchmark
    public void deephaven(InputProvider ip, final ReusableStorage storage, final Blackhole bh) throws Exception {
        final Results results = LargeTableDeephaven.read(ip.makeStream(), storage.results);
        bh.consume(results);
    }

    @Benchmark
    public void apache(final InputProvider ip, final ReusableStorage storage, final Blackhole bh) throws Exception {
        final Results results = LargeTableApache.read(ip.makeStream(), storage.results);
        bh.consume(results);
    }

    @Benchmark
    public void fastCsv(final InputProvider ip, final ReusableStorage storage, final Blackhole bh) throws Exception {
        final Results results = LargeTableFastCsv.read(ip.makeStream(), storage.results);
        bh.consume(results);
    }

    @Benchmark
    public void jackson(final InputProvider ip, final ReusableStorage storage, final Blackhole bh) throws Exception {
        final Results results = LargeTableJacksonCsv.read(ip.makeStream(), ip.headers, storage.results);
        bh.consume(results);
    }

    @Benchmark
    public void openCsv(final InputProvider ip, final ReusableStorage storage, final Blackhole bh) throws Exception {
        final Results results = LargeTableOpenCsv.read(ip.makeStream(), storage.results);
        bh.consume(results);
    }

    @Benchmark
    public void simpleFlatMapper(final InputProvider ip, final ReusableStorage storage, final Blackhole bh)
            throws Exception {
        final Results results = LargeTableSimpleFlatMapper.read(ip.makeStream(), storage.results);
        bh.consume(results);
    }

    @Benchmark
    public void superCsv(final InputProvider ip, final ReusableStorage storage, final Blackhole bh) throws Exception {
        final Results results = LargeTableSuperCsv.read(ip.makeStream(), storage.results);
        bh.consume(results);
    }
}
