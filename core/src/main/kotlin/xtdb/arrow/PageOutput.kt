package xtdb.arrow

import com.google.flatbuffers.FlatBufferBuilder
import com.google.protobuf.ByteString
import com.google.protobuf.UnsafeByteOperations
import org.apache.arrow.flatbuf.MessageHeader
import org.apache.arrow.flatbuf.RecordBatch
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector.ipc.message.ArrowBuffer
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.FBSerializables
import org.apache.arrow.vector.ipc.message.IpcOption
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.ipc.message.MessageSerializer.IPC_CONTINUATION_TOKEN
import org.apache.arrow.vector.types.pojo.Schema
import java.nio.ByteBuffer
import java.nio.ByteOrder.LITTLE_ENDIAN

/**
 * One page of an Arrow IPC stream, gathered on the heap as a relation's vectors write themselves into it
 * — see [VectorReader.write].
 *
 * Each buffer is copied out of Arrow memory as it is written, so the stream this yields owns its bytes and
 * may outlive the vectors it came from.
 * The framing is Arrow's own (`MessageSerializer`, `ArrowRecordBatch`'s aligned layout), so any Arrow
 * stream reader reads it.
 */
class PageOutput internal constructor() {
    private val nodes = mutableListOf<ArrowFieldNode>()
    private val buffers = mutableListOf<ArrowBuffer>()
    private val body = mutableListOf<ByteString>()
    private var bodyLength = 0L

    internal fun writeNode(length: Int, nullCount: Int) {
        nodes += ArrowFieldNode(length.toLong(), nullCount.toLong())
    }

    internal fun writeBuffer(byteLen: Long, fill: (ByteArray) -> Unit) {
        buffers += ArrowBuffer(bodyLength, byteLen)
        if (byteLen == 0L) return

        val paddedLen = byteLen.roundUpTo8()
        body += UnsafeByteOperations.unsafeWrap(ByteArray(Math.toIntExact(paddedLen)).also(fill))
        bodyLength += paddedLen
    }

    internal fun writeBuffer(src: ArrowBuf, srcOffset: Long, byteLen: Long) =
        writeBuffer(byteLen) { dst -> src.getBytes(srcOffset, dst, 0, byteLen.toInt()) }

    internal fun writeEmptyBuffer() = writeBuffer(0) {}

    internal fun toArrowStream(schema: Schema, rowCount: Int): ByteString =
        ByteString.copyFrom(
            listOf(
                frame(MessageSerializer.serializeMetadata(schema, IpcOption.DEFAULT)),
                frame(recordBatchMetadata(rowCount))
            ) + body + listOf(END_OF_STREAM)
        )

    // Built in the order `ArrowRecordBatch.writeTo` builds it, so the bytes match Arrow's own writer.
    private fun recordBatchMetadata(rowCount: Int): ByteBuffer {
        val builder = FlatBufferBuilder()

        RecordBatch.startNodesVector(builder, nodes.size)
        val nodesOffset = FBSerializables.writeAllStructsToVector(builder, nodes)
        RecordBatch.startBuffersVector(builder, buffers.size)
        val buffersOffset = FBSerializables.writeAllStructsToVector(builder, buffers)

        RecordBatch.startRecordBatch(builder)
        RecordBatch.addLength(builder, rowCount.toLong())
        RecordBatch.addNodes(builder, nodesOffset)
        RecordBatch.addBuffers(builder, buffersOffset)
        val batchOffset = RecordBatch.endRecordBatch(builder)

        return MessageSerializer.serializeMessage(
            builder, MessageHeader.RecordBatch, batchOffset, bodyLength, IpcOption.DEFAULT
        )
    }

    private companion object {
        private val END_OF_STREAM = ByteString.copyFrom(ByteArray(Int.SIZE_BYTES))

        private fun Long.roundUpTo8() = (this + 7) and 7L.inv()

        // `MessageSerializer.writeMessageBuffer`'s framing: continuation token, then the metadata length padded
        // so that the message ends on an 8-byte boundary, then the metadata and its padding.
        private fun frame(metadata: ByteBuffer): ByteString {
            val paddedLen = Math.toIntExact((metadata.remaining() + 8L).roundUpTo8() - 8)

            return UnsafeByteOperations.unsafeWrap(
                ByteArray(8 + paddedLen).also { bytes ->
                    ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN)
                        .putInt(IPC_CONTINUATION_TOKEN)
                        .putInt(paddedLen)
                        .put(metadata)
                }
            )
        }
    }
}

internal fun ByteArray.setIntLE(offset: Int, value: Int) {
    this[offset] = value.toByte()
    this[offset + 1] = (value ushr 8).toByte()
    this[offset + 2] = (value ushr 16).toByte()
    this[offset + 3] = (value ushr 24).toByte()
}
