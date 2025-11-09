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

/**
 * A WritableByteChannel that buffers writes in off-heap memory before writing to the underlying channel.
 * This reduces the number of small writes to the underlying channel, improving performance.
 * 
 * @param underlying The underlying WritableByteChannel to write to
 * @param bufferSize Size of the buffer in bytes
 * @param autoFlush If true, automatically flushes after each write() call completes. This ensures data
 *                  is available immediately but still benefits from buffering within each write() call.
 */
internal class BufferedWritableByteChannel(
    private val underlying: WritableByteChannel,
    bufferSize: Int = DEFAULT_BUFFER_SIZE,
    private val autoFlush: Boolean = false
) : WritableByteChannel {
    
    private val buffer = ByteBuffer.allocateDirect(bufferSize)
    private var isOpen = true
    
    override fun write(src: ByteBuffer): Int {
        checkOpen()
        var written = 0
        
        while (src.hasRemaining()) {
            if (!buffer.hasRemaining()) {
                flush()
            }
            
            val toWrite = minOf(src.remaining(), buffer.remaining())
            val limit = src.limit()
            src.limit(src.position() + toWrite)
            buffer.put(src)
            src.limit(limit)
            written += toWrite
        }
        
        if (autoFlush) {
            flush()
        }
        
        return written
    }
    
    private fun flush() {
        if (buffer.position() > 0) {
            buffer.flip()
            while (buffer.hasRemaining()) {
                underlying.write(buffer)
            }
            buffer.clear()
        }
    }
    
    override fun isOpen() = isOpen
    
    override fun close() {
        if (isOpen) {
            try {
                flush()
            } finally {
                isOpen = false
                underlying.close()
            }
        }
    }
    
    private fun checkOpen() {
        if (!isOpen) throw java.nio.channels.ClosedChannelException()
    }
    
    companion object {
        private const val DEFAULT_BUFFER_SIZE = 64 * 1024 // 64KB default buffer
    }
}

internal val ByteBuffer.asChannel get(): SeekableByteChannel = ByteBufferChannel(this)
internal val ByteArray.asChannel get(): SeekableByteChannel = ByteBuffer.wrap(this).asChannel