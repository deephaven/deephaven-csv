package io.deephaven.csv.densestorage;

import io.deephaven.csv.CsvSpecs;

/**
 * Constants that control the behavior of the {@link DenseStorageWriter} and {@link DenseStorageReader}.
 */
public class DenseStorageConstants {
    /**
     * When input strings are less than this threshold, we pack them tightly into a chunk. When they are greater than or
     * equal to this threshold, we allocate them directly as their own individual byte arrays.
     */
    public static final int LARGE_THRESHOLD = 1024;
    /**
     * Size of the "packed" byte blocks. The number chosen in somewhat arbitrary but it should be large-ish (100K? 1M?)
     * for performance and a decent multiple of LARGE_THRESHOLD to avoid wasting too much space at the end of each
     * block. By making it 1024x the size of LARGE_THRESHOLD, we can show that the fraction of wasted space at the end
     * of each block can never be more than (1/1024).
     */
    public static final int PACKED_QUEUE_SIZE = LARGE_THRESHOLD * 1024;
    /**
     * Size of the "large array queue" in terms of number of slots in the large array. We have arbitrarily chosen 1K
     * here.
     */
    public static final int LARGE_ARRAY_QUEUE_SIZE = 1024;
    /**
     * The threshold that will trigger a flush of the "large array queue" when it is exceeded. We have arbitrarily
     * chosen 1M here.
     */
    public static final int LARGE_ARRAY_CONTENT_LENGTH_FLUSH_THRESHOLD = LARGE_ARRAY_QUEUE_SIZE * LARGE_THRESHOLD;
    /**
     * This sentinel value is used to indicate that the next value being read is not bytes packed into a byte block but
     * rather its own byte array.
     */
    public static final int LARGE_BYTE_ARRAY_SENTINEL = -1;
    /**
     * This sentinel value is used to indicate that there are no more entries in the queue.
     */
    public static final int END_OF_STREAM_SENTINEL = -2;
    /**
     * The maximum number of data blocks that we allow to go unobserved before the blocking the QueueWriter. This is
     * only used when {@link CsvSpecs#concurrent()} is true.
     */
    public static final int MAX_UNOBSERVED_BLOCKS = 4;
}
