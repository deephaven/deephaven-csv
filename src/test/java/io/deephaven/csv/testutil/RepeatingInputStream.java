package io.deephaven.csv.testutil;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public final class RepeatingInputStream extends InputStream {
    private byte[] data;
    private final byte[] body;
    private int bodyRepeatsRemaining;
    private final byte[] tempBufferForRead;
    private int offset;

    public RepeatingInputStream(final String header, final String body, int bodyRepeats) {
        this.data = header.getBytes(StandardCharsets.UTF_8);
        this.body = body.getBytes(StandardCharsets.UTF_8);
        bodyRepeatsRemaining = bodyRepeats;
        tempBufferForRead = new byte[1];
        offset = 0;

    }

    @Override
    public int read() throws IOException {
        final int numBytes = read(tempBufferForRead, 0, 1);
        if (numBytes == 0) {
            return -1;
        }
        return tempBufferForRead[0] & 0xff;
    }

    @Override
    public int read(byte @NotNull [] b, int off, int len) {
        if (len == 0) {
            return 0;
        }
        while (offset == data.length) {
            if (bodyRepeatsRemaining == 0) {
                return -1;
            }
            data = body;
            offset = 0;
            --bodyRepeatsRemaining;
        }
        final int currentSize = data.length - offset;
        final int sizeToUse = Math.min(currentSize, len);
        System.arraycopy(data, offset, b, off, sizeToUse);
        offset += sizeToUse;
        return sizeToUse;
    }
}
