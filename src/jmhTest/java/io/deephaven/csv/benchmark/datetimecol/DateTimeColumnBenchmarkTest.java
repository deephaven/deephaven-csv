package io.deephaven.csv.benchmark.datetimecol;

import io.deephaven.csv.benchmark.datetimecol.DateTimeColumnBenchmark.InputProvider;
import io.deephaven.csv.benchmark.datetimecol.DateTimeColumnBenchmark.ReusableStorage;
import io.deephaven.csv.benchmark.util.BenchmarkResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DateTimeColumnBenchmarkTest {

    DateTimeColumnBenchmark benchmark;
    InputProvider inputProvider;
    ReusableStorage reusableStorage;

    @BeforeEach
    void setUp() {
        benchmark = new DateTimeColumnBenchmark();
        inputProvider = new InputProvider();
        reusableStorage = new ReusableStorage();
    }

    public interface Function {
        BenchmarkResult<long[]> execute(final InputProvider input, final ReusableStorage storage) throws Exception;
    }

    void runTest(Function function) throws Exception {
        final BenchmarkResult<long[]> result = function.execute(inputProvider, reusableStorage);
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
    void apacheDhDtp() throws Exception {
        runTest(benchmark::apacheDhDtp);
    }

    @Test
    void fastCsv() throws Exception {
        runTest(benchmark::fastCsv);
    }

    @Test
    void fastCsvDhDtp() throws Exception {
        runTest(benchmark::fastCsvDhDtp);
    }

    @Test
    void jackson() throws Exception {
        runTest(benchmark::jackson);
    }

    @Test
    void jacksonDhDtp() throws Exception {
        runTest(benchmark::jacksonDhDtp);
    }

    @Test
    void openCsv() throws Exception {
        runTest(benchmark::openCsv);
    }

    @Test
    void openCsvDhDtp() throws Exception {
        runTest(benchmark::openCsvDhDtp);
    }

    @Test
    void simpleFlatMapper() throws Exception {
        runTest(benchmark::simpleFlatMapper);
    }

    @Test
    void simpleFlatMapperDhDtp() throws Exception {
        runTest(benchmark::simpleFlatMapperDhDtp);
    }

    @Test
    void superCsv() throws Exception {
        runTest(benchmark::superCsv);
    }

    @Test
    void superCsvDhDtp() throws Exception {
        runTest(benchmark::superCsvDhDtp);
    }
}
