package io.deephaven.csv.reading;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.Objects;

/**
 * An InputStream that re-encodes another InputStream from one Charset to another. Uses CharsetDecoder and
 * CharsetEncoder for efficient streaming without loading the entire content into memory.
 */
final class ReEncodingInputStream extends InputStream {
    static final int MIN_BUFFER_SIZE = 2;

    private final InputStream source;
    private final CharsetDecoder decoder;
    private final CharsetEncoder encoder;

    private final ByteBuffer input;
    private final CharBuffer buffer;
    private final ByteBuffer output;
    private final byte[] read1;

    private boolean eof = false;
    private boolean fullyFlushed = false;

    ReEncodingInputStream(
            final InputStream source,
            final Charset sourceCharset,
            final Charset targetCharset,
            final int bufferSize) {
        if (bufferSize < MIN_BUFFER_SIZE) {
            throw new IllegalArgumentException(
                    String.format("Must have a buffer of at least %d characters to support potential surrogate pairs",
                            MIN_BUFFER_SIZE));
        }
        // There's an argument that we _could_ catch this case, but there are some cases where the caller knows best;
        // that is, the caller knows that source only contains a subset of sourceCharset that is known to encode
        // in the targetCharset.
        // if (!targetCharset.contains(sourceCharset)) {
        // throw new IllegalArgumentException();
        // }
        this.source = Objects.requireNonNull(source);
        decoder = sourceCharset.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        encoder = targetCharset.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        input = ByteBuffer.allocate((int) Math.ceil(bufferSize * sourceCharset.newEncoder().maxBytesPerChar()));
        input.flip();
        buffer = CharBuffer.allocate(bufferSize);
        output = ByteBuffer.allocate((int) Math.ceil(bufferSize * encoder.maxBytesPerChar()));
        output.flip();
        read1 = new byte[1];
    }

    @Override
    public int read() throws IOException {
        final int result = read(read1, 0, 1);
        return result < 0 ? result : (read1[0] & 0xFF);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return 0;
        }
        if (output.hasRemaining()) {
            int toRead = Math.min(output.remaining(), len);
            output.get(b, off, toRead);
            return toRead;
        }
        if (fullyFlushed) {
            return -1;
        }
        fillOutputBuffer();
        if (output.hasRemaining()) {
            int toRead = Math.min(output.remaining(), len);
            output.get(b, off, toRead);
            return toRead;
        }
        return -1;
    }

    private void fillOutputBuffer() throws IOException {
        output.compact();
        while (output.position() == 0 && !fullyFlushed) {
            if (!eof && (input.remaining() != input.capacity())) {
                input.compact();
                final int n = source.read(input.array(), input.position(), input.remaining());
                if (n == -1) {
                    eof = true;
                } else {
                    input.position(input.position() + n);
                }
                input.flip();
            }
            {
                final CoderResult decodeResult = decoder.decode(input, buffer, eof);
                if (decodeResult.isError()) {
                    decodeResult.throwException();
                }
            }
            buffer.flip();
            {
                final CoderResult encodeResult = encoder.encode(buffer, output, eof);
                if (encodeResult.isError()) {
                    encodeResult.throwException();
                }
            }
            buffer.compact();
            // If we've consumed all input, flush the encoder
            if (eof && !input.hasRemaining() && buffer.position() == 0) {
                final CoderResult flushResult = encoder.flush(output);
                if (flushResult.isError()) {
                    flushResult.throwException();
                }
                if (flushResult == CoderResult.UNDERFLOW) {
                    fullyFlushed = true;
                }
            }
            if (output.position() == 0 && eof) {
                fullyFlushed = true;
                break;
            }
        }
        output.flip();
    }

    @Override
    public void close() throws IOException {
        source.close();
    }
}
