package io.deephaven.csv.benchmark.largenumericonly;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(value = 1, jvmArgs = {"-Xms32G", "-Xmx32G"})
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
public class LargeNumericOnlyBenchmark {
    /**
     * Name of the test file.
     */
    private static final String LARGE_FILE_PATH = "/mnt/ramdisk/largefile_numericonly.csv";
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
                    "Long0", "Long1", "Long2", "Long3", "Double0", "Double1", "Double2", "Double3"
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
                    appendLong(sb, rng);
                    sb.append(',');
                    appendLong(sb, rng);
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
        final Results results = LargeNumericOnlyDeephaven.read(ip.makeStream(), storage.results);
        bh.consume(results);
    }
}

