package io.deephaven.csvbench;

public interface KosakianBenchmark {
    void setup();

    void checkResult();

    void teardown();
}
