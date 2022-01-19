package io.deephaven.csvbench;

import gnu.trove.list.array.*;

import java.util.ArrayList;
import java.util.function.Function;

public class MySinkFactory {
    public static io.deephaven.csv.sinks.SinkFactory create() {
        // TODO: we should pool the underlying TXList buffers
        return io.deephaven.csv.sinks.SinkFactory.of(
                MyByteSink::new, null,
                MyShortSink::new, null,
                MyIntSink::new, null,
                MyLongSink::new, null,
                MyFloatSink::new, null,
                MyDoubleSink::new, null,
                MyBooleanAsByteSink::new,
                MyCharSink::new, null,
                MyStringSink::new, null,
                MyDateTimeAsLongSink::new, null,
                MyTimestampAsLongSink::new, null);
    }

    public interface ResultProvider<TARRAY> {
        TARRAY toResult();
    }

    private static abstract class MySinkBase<TCOLLECTION, TARRAY>
            implements io.deephaven.csv.sinks.Sink<TARRAY>, ResultProvider<TARRAY> {
        protected final TCOLLECTION collection;
        protected int collectionSize;
        protected final FillOperation<TCOLLECTION> fillOperation;
        protected final SetOperation<TCOLLECTION, TARRAY> setOperation;
        protected final AddOperation<TCOLLECTION, TARRAY> addOperation;
        protected final Function<TCOLLECTION, TARRAY> toResultOperation;

        protected MySinkBase(TCOLLECTION collection, FillOperation<TCOLLECTION> fillOperation,
                SetOperation<TCOLLECTION, TARRAY> setOperation, AddOperation<TCOLLECTION, TARRAY> addOperation,
                Function<TCOLLECTION, TARRAY> toResultOperation) {
            this.collection = collection;
            this.fillOperation = fillOperation;
            this.setOperation = setOperation;
            this.addOperation = addOperation;
            this.toResultOperation = toResultOperation;
        }

        @Override
        public final void write(final TARRAY src, final boolean[] isNull, final long destBegin,
                final long destEnd, boolean appending) {
            if (destBegin == destEnd) {
                return;
            }
            final int size = Math.toIntExact(destEnd - destBegin);
            final int destBeginAsInt = Math.toIntExact(destBegin);
            final int destEndAsInt = Math.toIntExact(destEnd);

            if (!appending) {
                // Replacing.
                setOperation.apply(collection, destBeginAsInt, src, 0, size);
                return;
            }

            // Appending. First, if the new area starts beyond the end of the destination, pad the destination.
            if (collectionSize < destBegin) {
                fillOperation.apply(collection, collectionSize, destBeginAsInt);
                collectionSize = destBeginAsInt;
            }
            // Then do the append.
            addOperation.apply(collection, src, 0, size);
            collectionSize = destEndAsInt;
        }

        public final TARRAY toResult() {
            return toResultOperation.apply(collection);
        }

        /**
         * Meant to be paired with e.g. TDoubleArrayList.fill(int fromIndex, int toIndex, 0.0)
         */
        protected interface FillOperation<TCOLLECTION> {
            void apply(TCOLLECTION coll, int fromIndex, int toIndex);
        }

        /**
         * Meant to be paired with e.g. TDoubleArrayList.set(int offset, double[] values, int valOffset, int length)
         */
        protected interface SetOperation<TCOLLECTION, TARRAY> {
            void apply(TCOLLECTION coll, int offset, TARRAY values, int vallOffset, int length);
        }

        /**
         * Meant to be paired with e.g. TDoubleArrayList.add(double[] values, int offset, int length)
         */
        protected interface AddOperation<TCOLLECTION, TARRAY> {
            void apply(TCOLLECTION coll, TARRAY values, int offset, int length);
        }
    }

    private static abstract class MySourceAndSinkBase<TCOLLECTION, TARRAY> extends MySinkBase<TCOLLECTION, TARRAY>
            implements io.deephaven.csv.sinks.Source<TARRAY>, io.deephaven.csv.sinks.Sink<TARRAY> {
        private final ReadToArrayOperation<TCOLLECTION, TARRAY> readToArrayOperation;

        protected MySourceAndSinkBase(TCOLLECTION collection, FillOperation<TCOLLECTION> fillOperation,
                SetOperation<TCOLLECTION, TARRAY> setOperation, AddOperation<TCOLLECTION, TARRAY> addOperation,
                Function<TCOLLECTION, TARRAY> toResultOperation,
                ReadToArrayOperation<TCOLLECTION, TARRAY> readToArrayOperation) {
            super(collection, fillOperation, setOperation, addOperation, toResultOperation);
            this.readToArrayOperation = readToArrayOperation;
        }

        @Override
        public void read(TARRAY dest, boolean[] isNull, long srcBegin, long srcEnd) {
            if (srcBegin == srcEnd) {
                return;
            }
            final int size = Math.toIntExact(srcEnd - srcBegin);
            readToArrayOperation.apply(collection, dest, Math.toIntExact(srcBegin), 0, size);
        }

        /**
         * Meant to be paired with e.g. TDoubleArrayList.add(double[] dest, int source_pos, int dest_pos, int length)
         */
        private interface ReadToArrayOperation<TCOLLECTION, TARRAY> {
            void apply(TCOLLECTION coll, TARRAY dest, int source_pos_, int dest_pos, int length);
        }
    }

    private static class MyByteSinkBase extends MySourceAndSinkBase<TByteArrayList, byte[]> {
        public MyByteSinkBase() {
            super(new TByteArrayList(),
                    (dest, from, to) -> dest.fill(from, to, (byte) 0),
                    TByteArrayList::set,
                    TByteArrayList::add,
                    TByteArrayList::toArray,
                    TByteArrayList::toArray); // Note: different "toArray" from the above.
        }
    }

    private static final class MyByteSink extends MyByteSinkBase {
    }

    private static final class MyShortSink extends MySourceAndSinkBase<TShortArrayList, short[]> {
        public MyShortSink() {
            super(new TShortArrayList(),
                    (dest, from, to) -> dest.fill(from, to, (short) 0),
                    TShortArrayList::set,
                    TShortArrayList::add,
                    TShortArrayList::toArray,
                    TShortArrayList::toArray); // Note: different "toArray" from the above.
        }
    }

    private static final class MyIntSink extends MySourceAndSinkBase<TIntArrayList, int[]> {
        public MyIntSink() {
            super(new TIntArrayList(),
                    (dest, from, to) -> dest.fill(from, to, 0),
                    TIntArrayList::set,
                    TIntArrayList::add,
                    TIntArrayList::toArray,
                    TIntArrayList::toArray); // Note: different "toArray" from the above.
        }
    }

    private static class MyLongSinkBase extends MySourceAndSinkBase<TLongArrayList, long[]> {
        public MyLongSinkBase() {
            super(new TLongArrayList(),
                    (dest, from, to) -> dest.fill(from, to, 0L),
                    TLongArrayList::set,
                    TLongArrayList::add,
                    TLongArrayList::toArray,
                    TLongArrayList::toArray); // Note: different "toArray" from the above.
        }
    }

    private static final class MyLongSink extends MyLongSinkBase {
    }

    private static final class MyFloatSink extends MySinkBase<TFloatArrayList, float[]> {
        public MyFloatSink() {
            super(new TFloatArrayList(),
                    (dest, from, to) -> dest.fill(from, to, 0),
                    TFloatArrayList::set,
                    TFloatArrayList::add,
                    TFloatArrayList::toArray);
        }
    }

    private static final class MyDoubleSink extends MySinkBase<TDoubleArrayList, double[]> {
        public MyDoubleSink() {
            super(new TDoubleArrayList(),
                    (dest, from, to) -> dest.fill(from, to, 0),
                    TDoubleArrayList::set,
                    TDoubleArrayList::add,
                    TDoubleArrayList::toArray);
        }
    }

    private static final class MyBooleanAsByteSink extends MyByteSinkBase {
    }

    private static final class MyCharSink extends MySinkBase<TCharArrayList, char[]> {
        public MyCharSink() {
            super(new TCharArrayList(),
                    (coll, from, to) -> coll.fill(from, to, (char) 0),
                    TCharArrayList::set,
                    TCharArrayList::add,
                    TCharArrayList::toArray);
        }
    }

    private static final class MyStringSink extends MySinkBase<ArrayList<String>, String[]> {
        public MyStringSink() {
            super(new ArrayList<>(),
                    MyStringSink::fill,
                    MyStringSink::set,
                    MyStringSink::add,
                    c -> c.toArray(new String[0]));
        }

        private static void fill(final ArrayList<String> dest, final int from, final int to) {
            for (int current = from; current != to; ++current) {
                if (current < dest.size()) {
                    dest.set(current, null);
                } else {
                    dest.add(null);
                }
            }
        }

        private static void set(final ArrayList<String> dest, final int destOffset, final String[] src,
                final int srcOffset, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                dest.set(destOffset + ii, src[srcOffset + ii]);
            }
        }

        private static void add(final ArrayList<String> dest, final String[] src, final int srcOffset,
                final int size) {
            for (int ii = 0; ii < size; ++ii) {
                dest.add(src[srcOffset + ii]);
            }
        }
    }

    private static final class MyDateTimeAsLongSink extends MyLongSinkBase {
    }

    private static final class MyTimestampAsLongSink extends MyLongSinkBase {
    }
}
