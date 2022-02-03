package io.deephaven.csv.benchmark.intcol;

import io.deephaven.csv.benchmark.intcol.IntColumnBenchmark.InputProvider;
import io.deephaven.csv.benchmark.intcol.IntColumnBenchmark.ReusableStorage;
import io.deephaven.csv.benchmark.util.BenchmarkResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class IntColumnBenchmarkTest {

    IntColumnBenchmark benchmark;
    InputProvider inputProvider;
    ReusableStorage reusableStorage;

    @BeforeEach
    void setUp() {
        benchmark = new IntColumnBenchmark();
        inputProvider = new InputProvider();
        reusableStorage = new ReusableStorage();
    }

    public interface Function {
        BenchmarkResult<int[]> execute(final InputProvider input, final ReusableStorage storage) throws Exception;
    }

    void runTest(Function function) throws Exception {
        final BenchmarkResult<int[]> result = function.execute(inputProvider, reusableStorage);
        assertThat(result.columns()).isDeepEqualTo(inputProvider.tableMaker.expected());
    }

    @Test
    void deephaven() throws Exception {
        runTest(benchmark::deephaven);
    }

    @Test
    void apache() throws Exception {
        runTest(benchmark::apache);
    }

    @Test
    void fastCsv() throws Exception {
        runTest(benchmark::fastCsv);
    }

    @Test
    void jackson() throws Exception {
        runTest(benchmark::jackson);
    }

    @Test
    void openCsv() throws Exception {
        runTest(benchmark::openCsv);
    }

    @Test
    void simpleFlatMapper() throws Exception {
        runTest(benchmark::simpleFlatMapper);
    }

    @Test
    void superCsv() throws Exception {
        runTest(benchmark::superCsv);
    }
}
