package io.deephaven.csvbench;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;

public class KosakianBenchmarkRunner {
    public static void main(String[] args) {
        System.out.println("hello");
        try {
            // runBenchmark(new BenchmarkInts(),
            // BenchmarkInts::deephaven,
            // BenchmarkInts::fastCsv,
            // BenchmarkInts::apacheCommons,
            // BenchmarkInts::jacksonCsv,
            // BenchmarkInts::openCsv,
            // BenchmarkInts::simpleFlatMapper,
            // BenchmarkInts::superCsv,
            // BenchmarkInts::univocity);
            // runBenchmark(new BenchmarkDoubles(),
            // BenchmarkDoubles::deephaven,
            // BenchmarkDoubles::fastCsv,
            // BenchmarkDoubles::apacheCommons,
            // BenchmarkDoubles::jacksonCsv,
            // BenchmarkDoubles::openCsv,
            // BenchmarkDoubles::simpleFlatMapper,
            // BenchmarkDoubles::superCsv,
            // BenchmarkDoubles::univocity);
            // runBenchmark(new BenchmarkDateTimes(),
            // BenchmarkDateTimes::deephaven,
            // BenchmarkDateTimes::fastCsv,
            // BenchmarkDateTimes::apacheCommons,
            // BenchmarkDateTimes::jacksonCsv,
            // BenchmarkDateTimes::openCsv,
            // BenchmarkDateTimes::simpleFlatMapper,
            // BenchmarkDateTimes::superCsv,
            // BenchmarkDateTimes::univocity);
            runBenchmark(new BenchmarkStrings(),
                    BenchmarkStrings::deephaven,
                    BenchmarkStrings::fastCsv,
                    BenchmarkStrings::apacheCommons,
                    BenchmarkStrings::jacksonCsv,
                    BenchmarkStrings::openCsv,
                    BenchmarkStrings::simpleFlatMapper,
                    BenchmarkStrings::superCsv,
                    BenchmarkStrings::univocity);
        } catch (Exception e) {
            System.out.println("Caught exception " + e);
        }
        System.out.println("really done");
    }

    private static <T extends KosakianBenchmark> void runBenchmark(T benchmark,
            CheckedConsumer<T>... methods) throws Exception {
        for (CheckedConsumer<T> method : methods) {
            System.out.println(method);
            for (int ii = 0; ii < 3; ++ii) {
                System.out.println("Starting warmup " + ii);
                benchmark.setup();
                method.apply(benchmark);
                benchmark.checkResult();
                benchmark.teardown();
            }
            for (int ii = 0; ii < 5; ++ii) {
                System.out.println("Starting run " + ii);
                benchmark.setup();
                final LocalTime start = LocalTime.now();
                method.apply(benchmark);
                final LocalTime end = LocalTime.now();
                benchmark.checkResult();
                benchmark.teardown();
                final long millis = start.until(end, ChronoUnit.MILLIS);

                System.out.printf("Run %d took %g seconds\n", ii, millis / 1000.0);
            }
        }
        System.out.println("all done");
    }

    private interface CheckedConsumer<T> {
        void apply(T item) throws Exception;

    }
}
