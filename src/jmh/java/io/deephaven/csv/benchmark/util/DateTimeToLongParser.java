package io.deephaven.csv.benchmark.util;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.MutableLong;

import java.time.ZonedDateTime;

public interface DateTimeToLongParser {
    long parse(final String dateTimeText);

    class Standard implements DateTimeToLongParser {
        public long parse(final String s) {
            final ZonedDateTime zdt = ZonedDateTime.parse(s);
            final long zdtSeconds = zdt.toEpochSecond();
            final int zdtNanos = zdt.getNano();
            return zdtSeconds * 1_000_000_000 + zdtNanos;
        }
    }

    class Deephaven implements DateTimeToLongParser {
        private final Tokenizer tokenizer = new Tokenizer(null);
        private final ByteSlice bs = new ByteSlice();
        private final MutableLong result = new MutableLong();
        private byte[] bytes = new byte[0];

        public long parse(final String s) {
            if (s.length() > bytes.length) {
                bytes = new byte[s.length()];
            }
            for (int ii = 0; ii < s.length(); ++ii) {
                final char c = s.charAt(ii);
                if (c > 0x7f) {
                    throw new RuntimeException("Non-ASCII character encountered: not a DateTime");
                }
                bytes[ii] = (byte) c;
            }
            bs.reset(bytes, 0, s.length());
            if (!tokenizer.tryParseDateTime(bs, result)) {
                throw new RuntimeException("Can't parse '" + s + "' as DateTime");
            }
            return result.longValue();
        }
    }
}
