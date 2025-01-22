package xtdb.arrow

import io.netty.buffer.Unpooled
import org.apache.arrow.flatbuf.Footer
import org.apache.arrow.flatbuf.Message
import org.apache.arrow.flatbuf.RecordBatch
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.ForeignAllocation
import org.apache.arrow.memory.util.ByteFunctionHelpers
import org.apache.arrow.vector.ipc.message.ArrowBlock
import org.apache.arrow.vector.ipc.message.ArrowFooter
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.ipc.message.MessageSerializer.IPC_CONTINUATION_TOKEN
import org.apache.arrow.vector.ipc.message.MessageSerializer.deserializeRecordBatch
import xtdb.IllegalArgumentException
import java.nio.ByteBuffer
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

        val metadataBuf = nioBuffer(offset + prefixSize, (metadataLength - prefixSize).toInt())

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
            throw IllegalArgumentException.createNoKey("invalid Arrow IPC file format", emptyMap<String, String>())
    }

    fun ArrowBuf.readArrowFooter(): ArrowFooter {
        this.validateArrowMagic()

        val capacity = capacity()
        val magicLength = ARROW_MAGIC.size

        val footerSizeOffset = capacity - (Integer.BYTES + magicLength)
        val footerSize = getInt(footerSizeOffset)
        val footerPosition = footerSizeOffset - footerSize

        val footerBuffer = nioBuffer(footerPosition, footerSize)
        return ArrowFooter(Footer.getRootAsFooter(footerBuffer))
    }

    fun ArrowBuf.toArrowRecordBatchView(block: ArrowBlock): ArrowRecordBatch {
        val prefixSize = if (getInt(block.offset) == IPC_CONTINUATION_TOKEN) 8 else 4

        val messageBuffer = nioBuffer(
            block.offset + prefixSize,
            block.metadataLength - prefixSize
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

    fun ArrowBuf.toByteArray(): ByteArray {
        val bb = nioBuffer(0, capacity().toInt())
        return ByteArray(bb.remaining()).also { bb.get(it) }
    }
}