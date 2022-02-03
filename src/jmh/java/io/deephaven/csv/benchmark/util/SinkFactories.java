package io.deephaven.csv.benchmark.util;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.Source;
import io.deephaven.csv.sinks.SinkFactory;

import java.util.function.Supplier;

public final class SinkFactories {
    public static SinkFactory makeRecyclingSinkFactory(byte[][] byteBuffers, int[][] intBuffers, long[][] longBuffers,
            double[][] doubleBuffers, String[][] stringBuffers, long[][] datetimeAsLongBuffers) {
        return SinkFactory.of(
                null, null,
                null, null,
                makeSupplier(intBuffers), null,
                makeSupplier(longBuffers), null,
                null, null,
                makeSupplier(doubleBuffers), null,
                makeSupplier(byteBuffers),
                null, null,
                makeSupplier(stringBuffers), null,
                makeSupplier(datetimeAsLongBuffers), null,
                null, null);
    }

    private interface SourceSink<TARRAY> extends Source<TARRAY>, Sink<TARRAY> {
    }

    private static <TARRAY> Supplier<SourceSink<TARRAY>> makeSupplier(final TARRAY[] buffers) {
        return buffers == null ? null : new RecyclingSinkSupplier<>(buffers);
    }

    private static final class RecyclingSinkSupplier<TARRAY> implements Supplier<SourceSink<TARRAY>> {
        private final TARRAY[] buffers;
        private int nextIndex;

        public RecyclingSinkSupplier(TARRAY[] buffers) {
            this.buffers = buffers;
            nextIndex = 0;
        }

        @Override
        public SourceSink<TARRAY> get() {
            if (nextIndex == buffers.length) {
                throw new RuntimeException("Ran out of buffers");
            }
            final SourceSink<TARRAY> result = ArrayBackedSourceSink.of(buffers[nextIndex++]);
            return result;
        }
    }

    private static final class ArrayBackedSourceSink<TARRAY> implements SourceSink<TARRAY>, ArrayBacked<TARRAY> {
        public static <TARRAY> ArrayBackedSourceSink<TARRAY> of(final TARRAY storage) {
            return new ArrayBackedSourceSink<>(storage);
        }

        private final TARRAY storage;

        public ArrayBackedSourceSink(final TARRAY storage) {
            this.storage = storage;
        }

        @Override
        public void write(final TARRAY src, final boolean[] isNull_unused, final long destBegin, final long destEnd,
                final boolean appending_unused) {
            final int length = Math.toIntExact(destEnd) - Math.toIntExact(destBegin);
            System.arraycopy(src, 0, storage, Math.toIntExact(destBegin), length);
        }

        @Override
        public void read(final TARRAY dest, final boolean[] isNull_unused, final long srcBegin, final long srcEnd) {
            final int length = Math.toIntExact(srcEnd) - Math.toIntExact(srcBegin);
            System.arraycopy(storage, Math.toIntExact(srcBegin), dest, 0, length);
        }

        @Override
        public TARRAY getUnderlyingArray() {
            return storage;
        }
    }
}
