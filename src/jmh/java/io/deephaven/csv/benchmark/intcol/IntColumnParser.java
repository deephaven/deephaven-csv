package io.deephaven.csv.benchmark.intcol;

import io.deephaven.csvbench.MySinkFactory.ResultProvider;

import java.io.InputStream;

public interface IntColumnParser {

    ResultProvider<int[]> read(InputStream in) throws Exception;
}
