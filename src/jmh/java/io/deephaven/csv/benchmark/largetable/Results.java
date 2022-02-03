package io.deephaven.csv.benchmark.largetable;

public class Results {
    public final long[] timestamps;
    public final String[] strings;
    public final byte[] boolsAsBytes;
    public final long[] longs0;
    public final long[] longs1;
    public final double[] doubles0;
    public final double[] doubles1;
    public final double[] doubles2;

    public Results(final int rows) {
        timestamps = new long[rows];
        strings = new String[rows];
        boolsAsBytes = new byte[rows];
        longs0 = new long[rows];
        longs1 = new long[rows];
        doubles0 = new double[rows];
        doubles1 = new double[rows];
        doubles2 = new double[rows];
    }

    public Results(final long[] timestamps, final String[] strings, final byte[] boolsAsBytes, final long[] longs0,
            final long[] longs1, final double[] doubles0, final double[] doubles1, final double[] doubles2) {
        this.timestamps = timestamps;
        this.strings = strings;
        this.boolsAsBytes = boolsAsBytes;
        this.longs0 = longs0;
        this.longs1 = longs1;
        this.doubles0 = doubles0;
        this.doubles1 = doubles1;
        this.doubles2 = doubles2;
    }
}
