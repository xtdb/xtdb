package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.SeekableReadChannel
import org.apache.arrow.vector.ipc.message.ArrowFooter
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.ArrowUtil.arrowBufToRecordBatch
import xtdb.arrow.ArrowUtil.readArrowFooter
import java.nio.channels.SeekableByteChannel

sealed interface ArrowFileLoader : AutoCloseable {

    val schema: Schema
    val pageCount: Int
    fun openPage(idx: Int): ArrowRecordBatch

    companion object {
        @JvmStatic
        fun openFromChannel(al: BufferAllocator, ch: SeekableByteChannel): ArrowFileLoader =
            Channel(al, SeekableReadChannel(ch), ch.readArrowFooter())

        @JvmStatic
        fun openFromArrowBuf(buf: ArrowBuf): ArrowFileLoader =
            Buffer(buf, buf.readArrowFooter())
    }

    private class Channel(
        private val al: BufferAllocator, private val ch: SeekableReadChannel, private val footer: ArrowFooter,
    ) : ArrowFileLoader {

        override val schema: Schema get() = footer.schema
        override val pageCount: Int get() = footer.recordBatches.size

        override fun openPage(idx: Int): ArrowRecordBatch {
            val block = footer.recordBatches[idx]
            ch.setPosition(block.offset)

            return MessageSerializer.deserializeRecordBatch(ch, block, al)
                ?: error("Failed to deserialize record batch $idx, offset ${block.offset}")
        }

        override fun close() = ch.close()
    }

    private class Buffer(private val buf: ArrowBuf, private val footer: ArrowFooter) : ArrowFileLoader {

        init {
            buf.referenceManager.retain()
        }

        override val schema: Schema get() = footer.schema
        override val pageCount: Int get() = footer.recordBatches.size

        override fun openPage(idx: Int): ArrowRecordBatch {
            val block = footer.recordBatches[idx]

            return buf.arrowBufToRecordBatch(
                block.offset, block.metadataLength, block.bodyLength,
                "Failed to deserialize record batch $idx, offset ${block.offset}"
            )
        }

        override fun close() = buf.close()
    }
}
