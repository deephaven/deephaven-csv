package io.deephaven.csv.densestorage;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.util.Pair;

import java.util.concurrent.Semaphore;

/**
 * The {@link DenseStorageWriter} and {@link DenseStorageReader} work in tandem, forming a FIFO queue. The
 * {@link DenseStorageWriter} writes data, and the {@link DenseStorageReader} reads that data. If the
 * {@link DenseStorageReader} "catches up", it will block until the {@link DenseStorageWriter} provides more data, or
 * indicates that it is done (via the {@link #finish()} method. On the other hand, if the {@link DenseStorageReader}
 * falls behind (by a number of blocks defined by {@link DenseStorageConstants#MAX_UNOBSERVED_BLOCKS}), the
 * {@link DenseStorageWriter} will block until the {@link DenseStorageReader} starts to catch up again.
 *
 * <p>
 * This synchronization is done at block granularity, so the DenseStorageReader can only proceed when the
 * DenseStorageWriter has written at least a block of data or is finished. We allow multiple independent
 * {@link DenseStorageReader}s to consume the same underlying data. In our implementation this is used so that our type
 * inferencer can take a second pass over the input data if it needs to.
 *
 * <p>
 * The point of this object is to store a sequence of (UTF-8 character sequences represented as bytes), with less
 * overhead than there would be if we just passed arrays of {@link java.lang.String}. The problem with storing every
 * character sequence as a {@link java.lang.String} is:
 *
 * <ol>
 * <li>Per-object overhead (probably 8 or 16 bytes depending on pointer width)
 * <li>The memory cost of holding a reference to that String (again 4 or 8 bytes)
 * <li>The string has to know its length (4 bytes)
 * <li>Java characters are 2 bytes even though in practice many strings in our use case are ASCII-only and their chars
 * can fit in a byte. (Newer Java implementations can store text as bytes, eliminating this objection)
 * </ol>
 *
 * <p>
 * For small strings (e.g. the word "Price" or the input text "12345.6789") the overhead can be 100% or worse.
 *
 * <p>
 * Our use case is special in that we only need sequential access. i.e. we don't need random access into the sequence of
 * strings. So we can support a model where we can have a forward-only cursor moving over the sequence of strings. We
 * also don't need to give our caller a data structure that they can hold on to. The caller only gets a view (a slice)
 * of the current string data. The view is invalidated when they move to the next string. Both of these considerations
 * allow us to store strings as a packed array of UTF-8 bytes.
 *
 * <p>
 * Communication between the {@link DenseStorageWriter} and {@link DenseStorageReader} is done via a simple linked list
 * of immutable data. This gives us the following properties.
 *
 * <ol>
 * <li>A FIFO model where the reader (in a separate thread) can chase the writer but there is not an inordinate amount
 * of synchronization overhead (synchronization happens at the linked list node level, not the string level).
 * <li>Multiple Readers can pass over the same underlying data if necessary, e.g. for type inference.
 * <li>The linked-list structure permits the garbage collector to free nodes after a block has been processed (assuming
 * the data does not need to be kept around for a second type inference phase).
 * </ol>
 *
 * <p>
 * If you are familiar with the structure of our inference, you may initially think that this reader-chasing-writer
 * garbage collection trick doesn't buy us much because we have a two-phase parser. However, it is helpful in the
 * following important case. When the inferencer has gotten to the last parser in its set of allowable parsers
 * (typically the String parser, or when the caller has specified a specific parser and therefore there is only one
 * parser), the code recognizes that there is no need for a second phase. In this case the reader generally stays caught
 * up with the reader and blocks are freed as they are consumed.
 *
 * <p>
 * Logically, the implementation manages two queues: a "packed" queue used to hold control words, and the UTF-8 bytes
 * for strings whose length is up to a certain threshold; and a "large array" queue used to hold individual byte[]
 * references to the UTF-8 representation of strings larger than the threshold. We do not attempt to pack large strings
 * into our packed queue because they would not likely pack into them tightly. Since these strings are large by
 * definition, we are not concerned about the overhead of storing them separately.
 *
 * <p>
 * These are the various write operations:
 *
 * <ul>
 * <li>Write a small string: The length (4 bytes, nonnegative) is written to the packed queue, and then the UTF-8 bytes
 * of the string are written.</li>
 * <li>Write a large string: A special sentinel value (4 bytes with value
 * DenseStorageConstants.LARGE_BYTE_ARRAY_SENTINEL) is written to the packed queue, and then the byte[] reference
 * holding the UTF-8 encoding of the string is appended to the large array queue.</li>
 * <li>End of input: A special sentinel value (4 bytes, with value DenseStorageConstants.END_OF_STREAM_SENTINEL) is
 * written to the packed queue.</li>
 * </ul>
 *
 * <p>
 * The two queues are backed by Java arrays (of type byte[] and byte[][] respectively). These storage arrays typically
 * fill at different rates. To accommodate this while still using storage efficiently, the {@link DenseStorageWriter}
 * and {@link DenseStorageReader} keep track of the slice of the buffer that they are allowed to write to, or allowed to
 * read from, respectively. For example if the packed buffer fills up but the large array doesn't, the
 * {@link DenseStorageWriter} will create a {@link QueueNode} representing a slice of the whole packed buffer, but only
 * the filled part of the large array. Then the {@link DenseStorageWriter} will allocate a new packed buffer, starting
 * to fill it from the beginning, but it will continue to fill the large array from where it left off.
 *
 * <p>
 * Put another way, the {@link DenseStorageWriter#packedBuffer} can be considered to be split into multiple regions of
 * interest. The bytes in the half-open interval [0, {@link DenseStorageWriter#packedBegin}) have all been given away to
 * the {@link DenseStorageReader}, via the {@link QueueNode}, in one or multiple slices, and must not be touched
 * further. On the other hand the bytes in the half-open interval [{@link DenseStorageWriter#packedBegin},
 * packedBuffer.length) are private to the {@link DenseStorageWriter}, and can be modified at will. The variable
 * {@link DenseStorageWriter#packedCurrent} is an index to the next byte that the {@link DenseStorageWriter} plans to
 * write to. When {@link DenseStorageWriter#packedCurrent} becomes equal to packedBuffer.length, then the buffer is
 * full. In this case it will be flushed to a {@link QueueNode} and a fresh buffer will be allocated, with
 * {@link DenseStorageWriter#packedBegin} and {@link DenseStorageWriter#packedCurrent} set to 0.
 *
 * <p>
 * The buffer {@link DenseStorageWriter#largeArrayBuffer} follows the same rules as the above.
 *
 * <p>
 * A further note on synchronization. There is one DenseStorageWriter and potentially several
 * DenseStorageReaders in the system. A DenseStorageWriter has arrays (for its packed subqueue and its
 * largeArray subqueue) that are private to it, that it fills up at its leisure. It has a representation
 * for what "slice" of these subqueues it uniquely owns and can freely write to without synchronization.
 * Initially, that slice represents the whole array, but successive prefixes of that slice are repeatedly
 * given away to the DenseStorageReaders (via the QueueNode) as time goes on. The agreement beetween
 * the DenseStorageWriters and DenseStorageReaders is the DenseStorageWriter can do whatever it
 * wants to the slice it owns; meanwhile the DenseStorageReaders can do whatever *read-only* operation
 * they want on the slices they receive.
 *
 * <p>
 * At some point, according to its logic, the DenseStorageWriter will have partially or completely filled up its slices
 * in these two subqueues, and will want to share these slices with all the DenseStorageReaders.
 * In this case it removes a prefix from its slice for the packedArray subqueue and its slice for the
 * largeArray subqueu, and transmits those prefixes to the DenseStorageReaders. Once the DenseStorageWriter
 * has given away its whole array in this slicewise fashion, it allocates a fresh array for itself and
 * starts over.
 *
 * <p>
 * Conceptually these slices that have been shared are now under a shared ownership model: no one is allowed to write to them
 * ever again (this is enforced by convention). The slices are communicated via a QueueNode, which has
 * mostly final fields, again indicating that this is a shared, read-only structure.
 * However, there are two fields in the QueueNode that are mutable and hence controlled by synchronization.
 * Those fields are {@link QueueNode#next} and {@link QueueNode#appendHasBeenObserved}. Anyone
 * reading or writing those fields needs to do so under synchronization. The DenseStorageWriter
 * and all the DenseStorageReaders share an object called "syncRoot" that is used for this purpose.
 */
public final class DenseStorageWriter {
    /** Constructor */
    public static Pair<DenseStorageWriter, DenseStorageReader> create(final boolean concurrent) {
        final int maxUnobservedBlocks = concurrent ? DenseStorageConstants.MAX_UNOBSERVED_BLOCKS : Integer.MAX_VALUE;
        final Object syncRoot = new Object();
        final Semaphore semaphore = new Semaphore(maxUnobservedBlocks);
        // A placeholder node to hold the "next" field for both writer and reader.
        final QueueNode headNode = new QueueNode(null, 0, 0, null, 0, 0);
        final DenseStorageWriter writer = new DenseStorageWriter(syncRoot, semaphore, headNode);
        final DenseStorageReader reader = new DenseStorageReader(syncRoot, semaphore, headNode);
        return new Pair<>(writer, reader);
    }

    private final Object syncRoot;
    private final Semaphore semaphore;
    private QueueNode tail;

    private byte[] packedBuffer = new byte[DenseStorageConstants.PACKED_QUEUE_SIZE];
    private int packedBegin = 0;
    private int packedCurrent = 0;

    private byte[][] largeArrayBuffer = new byte[DenseStorageConstants.LARGE_ARRAY_QUEUE_SIZE][];
    private int largeArrayBegin = 0;
    private int largeArrayCurrent = 0;

    private final ByteSlice controlWordByteSlice = new ByteSlice(new byte[4], 0, 4);

    public DenseStorageWriter(Object syncRoot, Semaphore semaphore, QueueNode tail) {
        this.syncRoot = syncRoot;
        this.semaphore = semaphore;
        this.tail = tail;
    }

    /**
     * Append a {@link ByteSlice} to the queue. The data will be diverted to one of the two specialized underlying
     * queues, depending on its size.
     */
    public void append(final ByteSlice bs) {
        final int size = bs.size();
        if (size >= DenseStorageConstants.LARGE_THRESHOLD) {
            final byte[] largeArray = new byte[size];
            bs.copyTo(largeArray, 0);
            addControlWord(DenseStorageConstants.LARGE_BYTE_ARRAY_SENTINEL);
            addLargeArray(largeArray);
        } else {
            addControlWord(size);
            addBytes(bs);
        }
    }

    /** Call this method to indicate when you are finished writing to the queue. */
    public void finish() {
        addControlWord(DenseStorageConstants.END_OF_STREAM_SENTINEL);
        flush();
    }

    private void addControlWord(int controlWord) {
        final byte[] data = controlWordByteSlice.data();
        data[0] = (byte) controlWord;
        data[1] = (byte) (controlWord >>> 8);
        data[2] = (byte) (controlWord >>> 16);
        data[3] = (byte) (controlWord >>> 24);
        addBytes(controlWordByteSlice);
    }

    private void addBytes(ByteSlice bs) {
        final int sliceSize = bs.size();
        if (sliceSize == 0) {
            return;
        }
        assert sliceSize <= DenseStorageConstants.PACKED_QUEUE_SIZE;

        if (packedCurrent + sliceSize > packedBuffer.length) {
            flush();
            packedBuffer = new byte[DenseStorageConstants.PACKED_QUEUE_SIZE];
            packedBegin = 0;
            packedCurrent = 0;
        }
        bs.copyTo(packedBuffer, packedCurrent);
        packedCurrent += sliceSize;
    }

    private void addLargeArray(byte[] largeArray) {
        if (largeArrayCurrent == largeArrayBuffer.length) {
            flush();
            largeArrayBuffer = new byte[DenseStorageConstants.LARGE_ARRAY_QUEUE_SIZE][];
            largeArrayBegin = 0;
            largeArrayCurrent = 0;
        }
        largeArrayBuffer[largeArrayCurrent] = largeArray;
        ++largeArrayCurrent;
    }

    private void flush() {
        // This new node now owns the following slices (these are half-open intervals)
        // packedBuffer[packedBegin...packedCurrent)
        // largeArrayBuffer[largeArrayBegin...largeArrayCurrent)
        final QueueNode newNode = new QueueNode(
                packedBuffer, packedBegin, packedCurrent,
                largeArrayBuffer, largeArrayBegin, largeArrayCurrent);

        // DenseStorageWriter now owns suffix of the buffers after what the QueueNode owns.
        // packedBuffer[packedCurrent...packedBuffer.length)
        // largeArrayBuffer[largeArrayCurrent...largeArrayBuffer.length)
        packedBegin = packedCurrent;
        largeArrayBegin = largeArrayCurrent;

        appendNode(newNode);
    }

    private void appendNode(QueueNode newNode) {
        try {
            semaphore.acquire(1);
        } catch (InterruptedException ie) {
            throw new RuntimeException("Thread interrupted", ie);
        }

        synchronized (syncRoot) {
            if (tail.next != null) {
                throw new RuntimeException("next is already set");
            }
            tail.next = newNode;
            tail = newNode;
            syncRoot.notifyAll();
        }
    }
}
