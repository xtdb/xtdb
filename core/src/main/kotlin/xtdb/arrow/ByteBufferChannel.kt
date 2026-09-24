package xtdb.arrow

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.channels.WritableByteChannel

private class ByteBufferChannel(private val buf: ByteBuffer) : SeekableByteChannel {
    override fun read(dst: ByteBuffer): Int {
        val src = buf.slice().limit(dst.remaining())
        dst.put(src)
        val srcPosition = src.position()
        buf.position(buf.position() + srcPosition)
        return srcPosition
    }

    override fun write(src: ByteBuffer) = throw UnsupportedOperationException()

    override fun isOpen() = true
    override fun close() {}
    override fun position() = buf.position().toLong()
    override fun position(newPosition: Long) = apply { buf.position(newPosition.toInt()) }
    override fun size() = buf.limit().toLong()
    override fun truncate(size: Long) = throw UnsupportedOperationException()
}

internal class ByteArrayChannel : WritableByteChannel {
    private var buf = ByteArray(1024)
    private var size = 0

    override fun write(src: ByteBuffer): Int {
        val len = src.remaining()
        if (size + len > buf.size) buf = buf.copyOf(maxOf(buf.size * 2, size + len))
        src.get(buf, size, len)
        size += len
        return len
    }

    fun toByteArray(): ByteArray = buf.copyOf(size)

    override fun isOpen() = true
    override fun close() {}
}

internal val ByteBuffer.asChannel get(): SeekableByteChannel = ByteBufferChannel(this)
internal val ByteArray.asChannel get(): SeekableByteChannel = ByteBuffer.wrap(this).asChannel