package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.*
import org.apache.arrow.vector.types.pojo.Schema
import java.nio.channels.WritableByteChannel
import java.util.*

private val MAGIC = "ARROW1".toByteArray()

class Relation(val vectors: SequencedMap<String, Vector>, var rowCount: Int = 0) : AutoCloseable {

    constructor(vectors: List<Vector>, rowCount: Int = 0)
            : this(vectors.associateByTo(linkedMapOf()) { it.name }, rowCount)

    fun endRow() = ++rowCount

    inner class Unloader internal constructor(private val ch: WriteChannel) : AutoCloseable {

        private val vectors = this@Relation.vectors.values
        private val schema = Schema(vectors.map { it.field.arrowField })
        private val recordBlocks = mutableListOf<ArrowBlock>()

        init {
            ch.write(MAGIC)
            ch.align()
            MessageSerializer.serialize(ch, schema)
        }

        fun writeBatch() {
            val nodes = mutableListOf<ArrowFieldNode>()
            val buffers = mutableListOf<ArrowBuf>()

            vectors.forEach { it.unloadBatch(nodes, buffers) }

            ArrowRecordBatch(rowCount, nodes, buffers).use { recordBatch ->
                MessageSerializer.serialize(ch, recordBatch)
                    .also { recordBlocks.add(it) }
            }
        }

        fun endStream() {
            ch.writeIntLittleEndian(MessageSerializer.IPC_CONTINUATION_TOKEN)
            ch.writeIntLittleEndian(0)
        }

        fun endFile() {
            endStream()

            val footerStart = ch.currentPosition
            ch.write(ArrowFooter(schema, emptyList(), recordBlocks), false)

            val footerLength = ch.currentPosition - footerStart
            check(footerLength > 0) { "Footer length must be positive" }
            ch.writeIntLittleEndian(footerLength.toInt())
            ch.write(MAGIC)
        }

        override fun close() {
            ch.close()
        }
    }

    fun startUnload(ch: WritableByteChannel) = Unloader(WriteChannel(ch))

    /**
     * Resets the row count and all vectors, leaving the buffers allocated.
     */
    fun reset() {
        vectors.forEach { (_, vec) -> vec.reset() }
        rowCount = 0
    }

    override fun close() {
        vectors.forEach { (_, vec) -> vec.close() }
    }

    operator fun get(s: String) = vectors[s]
}
