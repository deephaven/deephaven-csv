package io.deephaven.csv.benchmark.util;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.MutableLong;

import java.time.ZonedDateTime;

public interface DateTimeToLongParser {
    long parse(final String dateTimeText);

    /**
     * An implementation that delegates to {@link ZonedDateTime#parse(CharSequence)}.
     */
    enum Standard implements DateTimeToLongParser {
        INSTANCE;

        public long parse(final String s) {
            final ZonedDateTime zdt = ZonedDateTime.parse(s);
            final long zdtSeconds = zdt.toEpochSecond();
            final int zdtNanos = zdt.getNano();
            return zdtSeconds * 1_000_000_000 + zdtNanos;
        }
    }

    /**
     * An stateful implementation that delegates to {@link Tokenizer#tryParseDateTime(ByteSlice, MutableLong)}. Not
     * thread-safe.
     */
    class Deephaven implements DateTimeToLongParser {

        private final Tokenizer tokenizer;
        private final ByteSlice bs = new ByteSlice();
        private final MutableLong result = new MutableLong();
        private byte[] bytes = new byte[0];

        public Deephaven() {
            this(new Tokenizer(null));
        }

        public Deephaven(Tokenizer tokenizer) {
            this.tokenizer = tokenizer;
        }

        public long parse(final String s) {
            final int L = s.length();
            if (L > bytes.length) {
                bytes = new byte[L];
            }
            for (int ii = 0; ii < L; ++ii) {
                final char c = s.charAt(ii);
                if (c > 0x7f) {
                    throw new RuntimeException("Non-ASCII character encountered: not a DateTime");
                }
                bytes[ii] = (byte) c;
            }
            bs.reset(bytes, 0, L);
            if (!tokenizer.tryParseDateTime(bs, result)) {
                throw new RuntimeException("Can't parse '" + s + "' as DateTime");
            }
            return result.longValue();
        }
    }
}
