package io.deephaven.csv.benchmark.doublecol;


import io.deephaven.csv.benchmark.doublecol.DoubleColumnBenchmark.InputProvider;
import io.deephaven.csv.benchmark.doublecol.DoubleColumnBenchmark.ReusableStorage;
import io.deephaven.csv.benchmark.util.BenchmarkResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DoubleColumnBenchmarkTest {

    DoubleColumnBenchmark benchmark;
    InputProvider inputProvider;
    ReusableStorage reusableStorage;

    @BeforeEach
    void setUp() {
        benchmark = new DoubleColumnBenchmark();
        inputProvider = new InputProvider();
        reusableStorage = new ReusableStorage();
    }

    public interface Function {
        BenchmarkResult<double[]> execute(final InputProvider input, final ReusableStorage storage) throws Exception;
    }

    void runTest(Function function) throws Exception {
        final BenchmarkResult<double[]> result = function.execute(inputProvider, reusableStorage);
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
    void apacheFDP() throws Exception {
        runTest(benchmark::apacheFDP);
    }

    @Test
    void fastCsv() throws Exception {
        runTest(benchmark::fastCsv);
    }

    @Test
    void fastCsvFDP() throws Exception {
        runTest(benchmark::fastCsvFDP);
    }

    @Test
    void jackson() throws Exception {
        runTest(benchmark::jackson);
    }

    @Test
    void jacksonFDP() throws Exception {
        runTest(benchmark::jacksonFDP);
    }

    @Test
    void openCsv() throws Exception {
        runTest(benchmark::openCsv);
    }

    @Test
    void openCsvFDP() throws Exception {
        runTest(benchmark::openCsvFDP);
    }

    @Test
    void simpleFlatMapper() throws Exception {
        runTest(benchmark::simpleFlatMapper);
    }

    @Test
    void simpleFlatMapperFDP() throws Exception {
        runTest(benchmark::simpleFlatMapperFDP);
    }

    @Test
    void superCsv() throws Exception {
        runTest(benchmark::superCsv);
    }

    @Test
    void superCsvFDP() throws Exception {
        runTest(benchmark::superCsvFDP);
    }
}
