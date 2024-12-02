package xtdb.arrow

import org.apache.arrow.flatbuf.Message
import org.apache.arrow.flatbuf.RecordBatch
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.ipc.message.MessageSerializer

object ArrowUtil {

    @JvmStatic
    @JvmOverloads
    fun arrowBufToRecordBatch(
        buf: ArrowBuf,
        offset: Long,
        metadataLength: Int,
        bodyLength: Long,
        errorString: String? = null,
    ) : ArrowRecordBatch {
        val prefixSize =
            if (buf.getInt(offset) == MessageSerializer.IPC_CONTINUATION_TOKEN) 8L else 4L

        val metadataBuf = buf.nioBuffer(offset + prefixSize, (metadataLength - prefixSize).toInt())

        val bodyBuf = buf.slice(offset + metadataLength, bodyLength)
            .also { it.referenceManager.retain() }

        val msg = Message.getRootAsMessage(metadataBuf.asReadOnlyBuffer())
        val recordBatchFB = RecordBatch().also { msg.header(it) }

        return MessageSerializer.deserializeRecordBatch(recordBatchFB, bodyBuf
            ?: error(errorString ?: "Failed to deserialize record batch at offset $offset"))
    }
}