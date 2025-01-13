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
    fun arrowBufToRecordBatch(
        buf: ArrowBuf,
        offset: Long,
        metadataLength: Int,
        bodyLength: Long,
        errorString: String? = null,
    ): ArrowRecordBatch {
        val prefixSize = if (buf.getInt(offset) == IPC_CONTINUATION_TOKEN) 8L else 4L

        val metadataBuf = buf.nioBuffer(offset + prefixSize, (metadataLength - prefixSize).toInt())

        val bodyBuf = buf.slice(offset + metadataLength, bodyLength)
            .also { it.referenceManager.retain() }

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

    private fun validateArrowMagic(ipcFileFormatBuffer: ArrowBuf) {
        val capacity = ipcFileFormatBuffer.capacity()
        val magicLength = ARROW_MAGIC.size

        if (0 != ByteFunctionHelpers.compare(
                ipcFileFormatBuffer, (capacity - magicLength).toInt(), capacity.toInt(),
                ARROW_MAGIC, 0, magicLength
            )
        )
            throw IllegalArgumentException.createNoKey("invalid Arrow IPC file format", emptyMap<String, String>())
    }

    fun readArrowFooter(ipcFileFormatBuffer: ArrowBuf): ArrowFooter {
        validateArrowMagic(ipcFileFormatBuffer)

        val capacity = ipcFileFormatBuffer.capacity()
        val magicLength = ARROW_MAGIC.size

        // Calculate positions
        val footerSizeOffset = capacity - (Integer.BYTES + magicLength)
        val footerSize = ipcFileFormatBuffer.getInt(footerSizeOffset)
        val footerPosition = footerSizeOffset - footerSize

        // Get footer buffer and create Arrow Footer
        val footerBuffer = ipcFileFormatBuffer.nioBuffer(footerPosition, footerSize)
        return ArrowFooter(Footer.getRootAsFooter(footerBuffer))
    }

    /**
     * Creates an Arrow record batch view from the given block and buffer
     */
    fun toArrowRecordBatchView(block: ArrowBlock, buffer: ArrowBuf): ArrowRecordBatch {
        // Determine prefix size based on continuation token
        val prefixSize = if (buffer.getInt(block.offset) == IPC_CONTINUATION_TOKEN) 8 else 4

        // Create message buffer and get record batch header
        val messageBuffer = buffer.nioBuffer(
            block.offset + prefixSize,
            block.metadataLength - prefixSize
        )

        val batch = RecordBatch()
        Message.getRootAsMessage(messageBuffer).header(batch)

        // Create and retain body buffer
        val bodyBuffer = buffer.slice(block.offset + block.metadataLength, block.bodyLength)
            .apply { referenceManager.retain() }

        return try {
            deserializeRecordBatch(batch, bodyBuffer)
        } catch (e: Exception) {
            bodyBuffer.referenceManager.release()
            throw e
        }
    }

    @JvmOverloads
    fun toArrowBufView(
        allocator: BufferAllocator,
        nioBuffer: ByteBuffer,
        onRelease: (() -> Unit)? = null
    ): ArrowBuf {
        val nettyBuf = if (nioBuffer.isDirect && nioBuffer.position() == 0) {
            // Use the buffer directly if it's a direct buffer with position 0
            Unpooled.wrappedBuffer(nioBuffer)
        } else {
            // Create a new direct buffer and copy the content
            val size = nioBuffer.remaining()
            val newBuffer = Unpooled.directBuffer(size)
            val bb = newBuffer.nioBuffer(0, size)
            bb.put(nioBuffer.duplicate())
            bb.clear()
            newBuffer
        }

        try {
            // Create a custom ForeignAllocation object
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

    fun arrowBufToByteArray(arrowBuf: ArrowBuf): ByteArray {
        val bb = arrowBuf.nioBuffer(0, arrowBuf.capacity().toInt())
        val ba = ByteArray(bb.remaining())
        bb.get(ba)
        return ba
    }
}