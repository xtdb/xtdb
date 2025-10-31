package xtdb.arrow

import io.netty.buffer.Unpooled
import org.apache.arrow.flatbuf.Footer.getRootAsFooter
import org.apache.arrow.flatbuf.Message
import org.apache.arrow.flatbuf.RecordBatch
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.ForeignAllocation
import org.apache.arrow.memory.util.ByteFunctionHelpers
import org.apache.arrow.vector.ipc.message.ArrowBlock
import org.apache.arrow.vector.ipc.message.ArrowFooter
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.ipc.message.MessageSerializer.IPC_CONTINUATION_TOKEN
import org.apache.arrow.vector.ipc.message.MessageSerializer.deserializeRecordBatch
import xtdb.error.Incorrect
import java.lang.foreign.MemorySegment
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.StandardCharsets

object ArrowUtil {

    @JvmStatic
    @JvmOverloads
    fun ArrowBuf.arrowBufToRecordBatch(
        offset: Long,
        metadataLength: Int,
        bodyLength: Long,
        errorString: String? = null,
    ): ArrowRecordBatch {
        val prefixSize = if (getInt(offset) == IPC_CONTINUATION_TOKEN) 8L else 4L

        val metadataBuf = toByteBuffer(offset + prefixSize, metadataLength - prefixSize)

        val bodyBuf = slice(offset + metadataLength, bodyLength).also { it.referenceManager.retain() }

        try {
            val msg = Message.getRootAsMessage(metadataBuf.asReadOnlyBuffer())
            val recordBatchFB = RecordBatch().also { msg.header(it) }

            return deserializeRecordBatch(
                recordBatchFB,
                bodyBuf ?: error(errorString ?: "Failed to deserialize record batch at offset $offset")
            )

        } catch (t: Throwable) {
            bodyBuf.referenceManager.release()
            throw t
        }
    }

    private val ARROW_MAGIC = "ARROW1".toByteArray(StandardCharsets.UTF_8)

    private fun ArrowBuf.validateArrowMagic() {
        val capacity = capacity()
        val magicLength = ARROW_MAGIC.size

        if (0 != ByteFunctionHelpers.compare(
                this, (capacity - magicLength).toInt(), capacity.toInt(),
                ARROW_MAGIC, 0, magicLength
            )
        )
            throw Incorrect("invalid Type IPC file format", errorCode = "xtdb/invalid-arrow-magic")
    }

    internal fun ArrowBuf.readArrowFooter(): ArrowFooter {
        val magicBytes = ByteArray(Int.SIZE_BYTES + MAGIC.size)
        val footerLengthOffset = capacity() - magicBytes.size
        getBytes(footerLengthOffset, magicBytes)

        require(MAGIC.contentEquals(magicBytes.copyOfRange(Int.SIZE_BYTES, magicBytes.size))) {
            "missing magic number at end of Type file"
        }

        val footerLength = MessageSerializer.bytesToInt(magicBytes)
        require(footerLength > 0) { "Footer length must be positive" }
        require(footerLength + MAGIC.size * 2 + Int.SIZE_BYTES <= capacity()) { "Footer length exceeds file size" }

        val footerBuffer = ByteBuffer.allocate(footerLength)
        getBytes(footerLengthOffset - footerLength, footerBuffer)
        footerBuffer.flip()
        return ArrowFooter(getRootAsFooter(footerBuffer))
    }

    internal fun SeekableByteChannel.readArrowFooter(): ArrowFooter {
        require(size() > MAGIC.size * 2 + 4) { "File is too small to be an Type file" }

        val buf = ByteBuffer.allocate(Int.SIZE_BYTES + MAGIC.size)
        val footerLengthOffset = size() - buf.remaining()
        position(footerLengthOffset)
        read(buf)
        buf.flip()

        val array = buf.array()

        require(MAGIC.contentEquals(array.copyOfRange(Int.SIZE_BYTES, array.size))) {
            "missing magic number at end of Type file"
        }

        val footerLength = MessageSerializer.bytesToInt(array)
        require(footerLength > 0) { "Footer length must be positive" }
        require(footerLength + MAGIC.size * 2 + Int.SIZE_BYTES <= size()) { "Footer length exceeds file size" }

        val footerBuffer = ByteBuffer.allocate(footerLength)
        position(footerLengthOffset - footerLength)
        read(footerBuffer)
        footerBuffer.flip()
        return ArrowFooter(getRootAsFooter(footerBuffer))
    }

    fun ArrowBuf.toArrowRecordBatchView(block: ArrowBlock): ArrowRecordBatch {
        val prefixSize = if (getInt(block.offset) == IPC_CONTINUATION_TOKEN) 8 else 4

        val messageBuffer = toByteBuffer(
            block.offset + prefixSize,
            (block.metadataLength - prefixSize).toLong()
        )

        val batch = RecordBatch()
        Message.getRootAsMessage(messageBuffer).header(batch)

        val bodyBuffer = slice(block.offset + block.metadataLength, block.bodyLength)
            .apply { referenceManager.retain() }

        return try {
            deserializeRecordBatch(batch, bodyBuffer)
        } catch (e: Exception) {
            bodyBuffer.referenceManager.release()
            throw e
        }
    }

    @JvmOverloads
    fun ByteBuffer.openArrowBufView(
        allocator: BufferAllocator,
        onRelease: (() -> Unit)? = null
    ): ArrowBuf {
        val nettyBuf =
            if (isDirect && position() == 0) Unpooled.wrappedBuffer(this)
            else {
                // Create a new direct buffer and copy the content
                val size = remaining()
                Unpooled.directBuffer(size).also {
                    val bb = it.nioBuffer(0, size)
                    bb.put(duplicate())
                    bb.clear()
                }
            }

        try {
            val foreignAllocation = object : ForeignAllocation(nettyBuf.capacity().toLong(), nettyBuf.memoryAddress()) {
                override fun release0() {
                    nettyBuf.release()
                    onRelease?.invoke()
                }
            }

            return allocator.wrapForeignAllocation(foreignAllocation)

        } catch (e: Exception) {
            nettyBuf.release()
            throw e
        }
    }

    fun ArrowBuf.toByteArray() = ByteArray(capacity().toInt()).also { getBytes(0, it) }

    @Suppress("Since15") // IntelliJ flags this as unavailable, but it's actually available.
    fun ArrowBuf.toByteBuffer(start: Long = 0, len: Long = capacity()): ByteBuffer =
        MemorySegment.ofAddress(memoryAddress() + start).reinterpret(len).asByteBuffer()
}