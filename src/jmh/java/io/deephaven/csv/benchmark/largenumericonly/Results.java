package io.deephaven.csv.benchmark.largenumericonly;

public class Results {
    public final long[] longs0;
    public final long[] longs1;
    public final long[] longs2;
    public final long[] longs3;
    public final double[] doubles0;
    public final double[] doubles1;
    public final double[] doubles2;
    public final double[] doubles3;

    public Results(final int rows) {
        longs0 = new long[rows];
        longs1 = new long[rows];
        longs2 = new long[rows];
        longs3 = new long[rows];
        doubles0 = new double[rows];
        doubles1 = new double[rows];
        doubles2 = new double[rows];
        doubles3 = new double[rows];
    }

    public Results(long[] longs0, long[] longs1, long[] longs2, long[] longs3, double[] doubles0, double[] doubles1,
            double[] doubles2, double[] doubles3) {
        this.longs0 = longs0;
        this.longs1 = longs1;
        this.longs2 = longs2;
        this.longs3 = longs3;
        this.doubles0 = doubles0;
        this.doubles1 = doubles1;
        this.doubles2 = doubles2;
        this.doubles3 = doubles3;
    }
}
