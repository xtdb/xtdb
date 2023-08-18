package xtdb.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class WritableByteBufferChannel implements AutoCloseable {
    private final ByteArrayOutputStream baos;
    private final WritableByteChannel ch;

    private WritableByteBufferChannel(ByteArrayOutputStream baos, WritableByteChannel ch) {
        this.baos = baos;
        this.ch = ch;
    }

    public ByteBuffer getAsByteBuffer() {
        return ByteBuffer.wrap(baos.toByteArray());
    }

    public WritableByteChannel getChannel() {
        return ch;
    }

    public static WritableByteBufferChannel open() {
        var baos = new ByteArrayOutputStream();
        var ch = Channels.newChannel(baos);
        return new WritableByteBufferChannel(baos, ch);
    }

    @Override
    public void close() throws IOException {
        ch.close();
    }
}
