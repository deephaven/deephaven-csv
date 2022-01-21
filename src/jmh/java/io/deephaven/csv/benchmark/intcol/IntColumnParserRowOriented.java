package io.deephaven.csv.benchmark.intcol;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.util.MutableObject;
import io.deephaven.csvbench.MySinkFactory;
import io.deephaven.csvbench.MySinkFactory.ResultProvider;

import java.io.InputStream;
import java.util.Objects;

public abstract class IntColumnParserRowOriented implements IntColumnParser {

    interface Collector {

        void acceptRow(int col0);

        void acceptRows(int[] col0, int len);
    }

    @Override
    public final ResultProvider<int[]> read(InputStream in) throws Exception {
        final IntAdapter adapter = IntAdapter.create(MySinkFactory.create());
        readInto(in, adapter);
        return adapter.results();
    }

    public abstract void readInto(InputStream in, Collector collector) throws Exception;

    private static final class IntAdapter implements Collector {

        public static IntAdapter create(SinkFactory sinkFactory) {
            return new IntAdapter(sinkFactory.forInt(new MutableObject<>()));
        }

        private final Sink<int[]> sink;
        private final int[] src = new int[1];
        private final boolean[] isNull = null; // not checked in MySinkFactory
        private int ix;

        private IntAdapter(Sink<int[]> sink) {
            this.sink = Objects.requireNonNull(sink);
        }

        @Override
        public void acceptRow(int col0) {
            src[0] = col0;
            sink.write(src, isNull, ix, ix + 1, true);
            ++ix;
        }

        @Override
        public void acceptRows(int[] col0, int len) {
            sink.write(col0, isNull, ix, ix + len, true);
            ix += len;
        }

        public ResultProvider<int[]> results() {
            // noinspection unchecked
            return (ResultProvider<int[]>) sink;
        }
    }
}
