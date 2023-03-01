package io.deephaven.csv.densestorage;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.util.MutableObject;
import io.deephaven.csv.util.Pair;

/**
 * The DenseStorageWriter and {@link DenseStorageReader} work in tandem, forming a FIFO queue. The DenseStorageWriter
 * writes data, and the {@link DenseStorageReader} reads that data. If the {@link DenseStorageReader} "catches up", it
 * will block until the DenseStorageWriter provides more data, or indicates that it is done (via the {@link #finish()}
 * method. This synchronization is done at "block" granularity, so the DenseStorageReader can only proceed when the
 * DenseStorageWriter has written at least a "block" of data or is done. We allow multiple independent
 * {@link DenseStorageReader}s to consume the same underlying data. In our implementation this is used so our type
 * inferencer can take a second "pass" over the same input data.
 *
 * <p>
 * The point of this object is to store a sequence of (character sequences aka "strings", but not java.lang.String),
 * using a small fraction of overhead. The problem with storing every character sequence as a java.lang.String is:
 *
 * <ol>
 * <li>Per-object overhead (probably 8 or 16 bytes depending on pointer width)
 * <li>The memory cost of holding a reference to that String (again 4 or 8 bytes)
 * <li>The string has to know its length (4 bytes)
 * <li>Java characters are 2 bytes even though in practice many strings are ASCII-only and their chars can fit in a
 * byte. (Newer Java implementations can store text as bytes, eliminating this objection)
 * </ol>
 *
 * <p>
 * For small strings (say the word "hello" or the input text "12345.6789") the overhead can be 100% or worse.
 *
 * <p>
 * For our purposes we:
 *
 * <ol>
 * <li>Only need sequential access. i.e. we don't need random access into the sequence of "strings". So we can support a
 * model where we can have a forward-only cursor moving over the sequence of "strings".
 * <li>Don't need to give our caller a data structure that they can hold on to. The caller only gets a "view" (a slice)
 * of the current "string" data. The view is invalidated when they move to the next "string"
 * </ol>
 *
 * Furthermore we:
 *
 * <ol>
 * <li>Offer a FIFO model where the reader (in a separate thread) can chase the writer but there is not an inordinate
 * amount of synchronization overhead (synchronization happens at the block level, not the "string" level).
 * <li>Have the ability to make multiple Readers which pass over the same underlying data. This is our low-drama way of
 * allowing our client to make multiple passes over the data, without complicating the iteration interface, with, e.g.,
 * a reset method.
 * <li>Use a linked-list structure so that when all existing readers have move passed a block of data, that block can be
 * freed by the garbage collector without any explicit action taken by the reader.
 * </ol>
 *
 * If you are familiar with the structure of our inference, you may initially think that this reader-chasing-writer
 * garbage collection trick doesn't buy us much because we have a two-phase parser. However, when the inferencer has
 * gotten to the last parser in its set of allowable parsers (say, the String parser), or the user has specified that
 * there is only one parser for this column, then the code doesn't need to do any inference and can parse the column in
 * one pass. In this case, when the reader stays caught up with the writer, we are basically just buffering one block of
 * data, not the whole file.
 *
 * <p>
 * The implementation used here is to look at the "string" being added to the writer and categorize it along two
 * dimensions:
 *
 * <ul>
 * <li>Small vs large
 * <li>Byte vs char
 * </ul>
 *
 * These dimensions are broken out in the following way:
 * <ul>
 * <li>Small byte "strings" are packed into a byte block, and we maintain a linked list of these byte blocks.
 * <li>"Large" byte "strings" are stored directly, meaning a byte[] array is allocated for their data, then a reference
 * to that array is added to a byte-array block. (And again, we maintain a linked list of these byte-array blocks). It
 * is not typical for CSV data to contain a cell this large, but the feature is there for completeness. We do not want
 * want large "strings" to contaminate our packed byte blocks because they would not likely pack into them tightly (it
 * would become more likely to have allocated blocks with unused storage at the end, because the last big string
 * wouldn't fit in the current block). It's OK to keep them on their own because by definition, large "strings" are not
 * going to have much overhead, as a percentage of the size of their text content.
 * </ul>
 */
public final class DenseStorageWriter {
    /** Constructor */
    public static Pair<DenseStorageWriter, DenseStorageReader> create(final boolean concurrent) {
        final Pair<QueueWriter.IntWriter, QueueReader.IntReader> control =
                QueueWriter.IntWriter.create(DenseStorageConstants.CONTROL_QUEUE_SIZE, concurrent);
        final Pair<QueueWriter.ByteWriter, QueueReader.ByteReader> bytes =
                QueueWriter.ByteWriter.create(DenseStorageConstants.PACKED_QUEUE_SIZE, concurrent);
        final Pair<QueueWriter.ByteArrayWriter, QueueReader.ByteArrayReader> byteArrays =
                QueueWriter.ByteArrayWriter.create(DenseStorageConstants.ARRAY_QUEUE_SIZE, concurrent);

        final DenseStorageWriter writer = new DenseStorageWriter(control.first, bytes.first, byteArrays.first);
        final DenseStorageReader reader = new DenseStorageReader(control.second, bytes.second, byteArrays.second);
        return new Pair<>(writer, reader);
    }

    /**
     * The ints in this array indicate where the next item is stored:
     *
     * <ul>
     * <li>{@link DenseStorageConstants#LARGE_BYTE_ARRAY_SENTINEL}: {@link DenseStorageWriter#largeByteArrayWriter}.
     * <li>&gt; 0: {@link DenseStorageWriter#byteWriter} (the number of chars is equal to this value)
     * <li>== 0: no bytes, so they're not stored anywhere. Will be interpreted as a ByteSlice with arbitrary byte data
     * and length 0.
     * </ul>
     */
    private final QueueWriter.IntWriter controlWriter;
    /** Byte sequences < DENSE_THRESHOLD are compactly stored here */
    private final QueueWriter.ByteWriter byteWriter;
    /** Byte sequences >= DENSE_THRESHOLD are stored here */
    private final QueueWriter.ByteArrayWriter largeByteArrayWriter;

    private DenseStorageWriter(QueueWriter.IntWriter controlWriter, QueueWriter.ByteWriter byteWriter,
            QueueWriter.ByteArrayWriter largeByteArrayWriter) {
        this.controlWriter = controlWriter;
        this.byteWriter = byteWriter;
        this.largeByteArrayWriter = largeByteArrayWriter;
    }

    /**
     * Append a {@link ByteSlice} to the queue. The data will be diverted to one of the two specialized underlying
     * queues, depending on its size.
     */
    public void append(final ByteSlice bs) {
        final boolean fctrl, fdata;
        final int size = bs.size();
        if (size >= DenseStorageConstants.LARGE_THRESHOLD) {
            final byte[] data = new byte[size];
            bs.copyTo(data, 0);
            fdata = largeByteArrayWriter.addByteArray(data);
            fctrl = controlWriter.addInt(DenseStorageConstants.LARGE_BYTE_ARRAY_SENTINEL);
        } else {
            fdata = byteWriter.addBytes(bs);
            fctrl = controlWriter.addInt(size);
        }
        // If the control queue flushes, then flush the data queues, so the reader doesn't block for
        // a long time waiting for some unflushed data queue. Conversely, if some data queue flushes, then
        // flush the control queue for the same reasons. Importantly, we also want to do this because our
        // flow control is based on limiting the number of data queue blocks outstanding
        // (per DenseStorageConstants.MAX_UNOBSERVED_BLOCKS). We want to flush the control queue every
        // time we fill a block on the data queue, so the consumer has a chance to consume the data. If we
        // did not do this, in some cases the data queue would run too far ahead, the flow control would be invoked
        // to block the writer, but the reader would also be blocked because it is still waiting on control queue
        // notifications, which haven't arrived because the latest control queue block isn't full and hasn't
        // been flushed yet. See https://github.com/deephaven/deephaven-csv/issues/101.
        // One might worry that it is inefficient to flush a queue that is not full, but (a) in practice it
        // doesn't happen very often and (b) in our queue code, partially-filled blocks can share
        // non-overlapping parts (slices) of their underlying storage array, so it's not particularly wasteful.
        // Put another way, flushing an empty queue does nothing; flushing a partially-filled queue allocates
        // a new QueueNode but not a new underlying data array; flushing a full queue will allocate a new
        // QueueNode and a new underlying data array (btw, that allocation is lazily deferred until the next write).
        if (fctrl) {
            byteWriter.flush();
            largeByteArrayWriter.flush();
        } else if (fdata) {
            controlWriter.flush();
        }
    }

    /** Call this method to indicate when you are finished writing to the queue. */
    public void finish() {
        controlWriter.finish();
        byteWriter.finish();
        largeByteArrayWriter.finish();
    }
}
