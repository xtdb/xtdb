package xtdb.arrow

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel

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

internal val ByteBuffer.asChannel get(): SeekableByteChannel = ByteBufferChannel(this)
internal val ByteArray.asChannel get(): SeekableByteChannel = ByteBuffer.wrap(this).asChannel