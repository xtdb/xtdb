package xtdb.arrow

import org.apache.arrow.flatbuf.Footer
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.SeekableReadChannel
import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.*
import org.apache.arrow.vector.types.pojo.Schema
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
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

    private fun load(recordBatch: ArrowRecordBatch) {
        val nodes = recordBatch.nodes.toMutableList()
        val buffers = recordBatch.buffers.toMutableList()
        vectors.values.forEach { it.loadBatch(nodes, buffers) }

        require(nodes.isEmpty()) { "Unconsumed nodes: $nodes" }
        require(buffers.isEmpty()) { "Unconsumed buffers: $buffers" }

        rowCount = recordBatch.length
    }

    inner class Loader(
        private val al: BufferAllocator,
        private val ch: SeekableReadChannel,
        footer: ArrowFooter
    ) : AutoCloseable {
        val relation get() = this@Relation

        inner class LoaderBatch(private val idx: Int, private val block: ArrowBlock) {
            fun load() {
                ch.setPosition(block.offset)

                (MessageSerializer.deserializeRecordBatch(ch, block, al)
                    ?: error("Failed to deserialize record batch $idx, offset ${block.offset}"))

                    .use { batch -> this@Relation.load(batch) }
            }
        }

        val batches = footer.recordBatches.mapIndexed(::LoaderBatch)

        override fun close() {
            relation.close()
            ch.close()
        }
    }

    companion object {
        private fun SeekableReadChannel.readFooter(): ArrowFooter {
            val buf = ByteBuffer.allocate(Int.SIZE_BYTES + MAGIC.size)
            val footerLengthOffset = size() - buf.remaining()
            setPosition(footerLengthOffset)
            readFully(buf)
            buf.flip()

            val array = buf.array()

            require(MAGIC.contentEquals(array.copyOfRange(Int.SIZE_BYTES, array.size))) {
                "missing magic number at end of Arrow file"
            }

            val footerLength = MessageSerializer.bytesToInt(array)
            require(footerLength > 0) { "Footer length must be positive" }
            require(footerLength + MAGIC.size * 2 + Int.SIZE_BYTES <= size()) { "Footer length exceeds file size" }

            val footerBuffer = ByteBuffer.allocate(footerLength)
            setPosition(footerLengthOffset - footerLength)
            readFully(footerBuffer)
            footerBuffer.flip()
            return ArrowFooter(Footer.getRootAsFooter(footerBuffer))
        }

        fun load(al: BufferAllocator, ch: SeekableByteChannel): Loader {
            val readCh = SeekableReadChannel(ch)
            require(readCh.size() > MAGIC.size * 2 + 4) { "File is too small to be an Arrow file" }

            val footer = readCh.readFooter()
            val rel = Relation(footer.schema.fields.map { Field.from(it).newVector(al) })

            return rel.Loader(al, readCh, footer)
        }
    }

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
