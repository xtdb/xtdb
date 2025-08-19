package xtdb.arrow

import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.ArrowBlock
import org.apache.arrow.vector.ipc.message.ArrowFooter
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.trie.FileSize
import java.nio.channels.ClosedByInterruptException
import java.nio.channels.WritableByteChannel

internal val MAGIC = "ARROW1".toByteArray()

class ArrowUnloader private constructor(
    private val ch: WriteChannel, private val schema: Schema, private val mode: Mode
) : AutoCloseable {

    private val arrowBlocks = mutableListOf<ArrowBlock>()

    init {
        try {
            mode.start(ch)
            MessageSerializer.serialize(ch, schema)
        } catch (_: ClosedByInterruptException) {
            throw InterruptedException()
        }

    }

    enum class Mode {
        STREAM {
            override fun start(ch: WriteChannel) {
            }

            override fun end(ch: WriteChannel, schema: Schema, recordBlocks: MutableList<ArrowBlock>) {
                ch.writeIntLittleEndian(0)
            }
        },

        FILE {
            override fun start(ch: WriteChannel) {
                ch.write(MAGIC)
                ch.align()
            }

            override fun end(ch: WriteChannel, schema: Schema, recordBlocks: MutableList<ArrowBlock>) {
                STREAM.end(ch, schema, recordBlocks)

                val footerStart = ch.currentPosition
                ch.write(ArrowFooter(schema, emptyList(), recordBlocks), false)

                val footerLength = ch.currentPosition - footerStart
                check(footerLength > 0) { "Footer length must be positive" }
                ch.writeIntLittleEndian(footerLength.toInt())
                ch.write(MAGIC)
            }
        };

        abstract fun start(ch: WriteChannel)
        abstract fun end(ch: WriteChannel, schema: Schema, recordBlocks: MutableList<ArrowBlock>)
    }

    fun writeBatch(batch: ArrowRecordBatch) {
        try {
            MessageSerializer.serialize(ch, batch).also { arrowBlocks.add(it) }
        } catch (_: ClosedByInterruptException) {
            throw InterruptedException()
        }
    }

    fun end(): FileSize {
        mode.end(ch, schema, arrowBlocks)
        return ch.currentPosition
    }

    override fun close() {
        ch.close()
    }

    companion object {
        @JvmOverloads
        @JvmStatic
        fun open(ch: WritableByteChannel, schema: Schema, mode: Mode = Mode.FILE) =
            ArrowUnloader(WriteChannel(ch), schema, mode)
    }
}