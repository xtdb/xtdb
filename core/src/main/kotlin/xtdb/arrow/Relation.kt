package xtdb.arrow

import org.apache.arrow.flatbuf.Footer
import org.apache.arrow.flatbuf.Message
import org.apache.arrow.flatbuf.RecordBatch
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.SeekableReadChannel
import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.*
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.Vector.Companion.fromField
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.channels.WritableByteChannel
import java.util.*
import xtdb.vector.RelationReader as OldRelationReader

private val MAGIC = "ARROW1".toByteArray()

class Relation(val vectors: SequencedMap<String, Vector>, override var rowCount: Int = 0) : RelationReader {

    @JvmOverloads
    constructor(vectors: List<Vector>, rowCount: Int = 0)
            : this(vectors.associateByTo(linkedMapOf()) { it.name }, rowCount)

    @JvmOverloads
    constructor(allocator: BufferAllocator, schema: Schema, rowCount: Int = 0)
            : this(allocator, schema.fields, rowCount)

    @JvmOverloads
    constructor(allocator: BufferAllocator, fields: List<Field>, rowCount: Int = 0)
            : this(fields.map { fromField(allocator, it) }, rowCount)

    fun endRow() = ++rowCount

    override fun iterator() = vectors.values.iterator()

    inner class Unloader internal constructor(private val ch: WriteChannel) : AutoCloseable {

        private val vectors = this@Relation.vectors.values
        private val schema = Schema(vectors.map { it.field })
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

    interface Loader : AutoCloseable {
        val schema: Schema
        val batchCount: Int
        fun loadBatch(idx: Int, al: BufferAllocator): Relation
        fun loadBatch(idx: Int, rel: Relation): Relation
    }

    private class ChannelLoader(
        private val al: BufferAllocator,
        private val ch: SeekableReadChannel,
        footer: ArrowFooter
    ) : Loader {
        inner class LoaderBatch(private val idx: Int, private val block: ArrowBlock) {
            fun load(rel: Relation): Relation {
                ch.setPosition(block.offset)

                (MessageSerializer.deserializeRecordBatch(ch, block, al)
                    ?: error("Failed to deserialize record batch $idx, offset ${block.offset}"))

                    .use { batch -> rel.load(batch) }

                return rel
            }
        }

        override val schema: Schema = footer.schema
        private val batches = footer.recordBatches.mapIndexed(::LoaderBatch)

        override val batchCount = batches.size

        override fun loadBatch(idx: Int, al: BufferAllocator) = loadBatch(idx, Relation(al, schema))
        override fun loadBatch(idx: Int, rel: Relation) = batches[idx].load(rel)

        override fun close() {
            ch.close()
        }
    }

    private class BufferLoader(
        private val buf: ArrowBuf,
        footer: ArrowFooter
    ) : Loader {
        override val schema: Schema = footer.schema

        inner class LoaderBatch(private val idx: Int, private val block: ArrowBlock) {

            fun load(rel: Relation): Relation {
                val prefixSize =
                    if (buf.getInt(block.offset) == MessageSerializer.IPC_CONTINUATION_TOKEN) 8L else 4L

                val metadataBuf = buf.nioBuffer(block.offset + prefixSize, block.metadataLength - prefixSize.toInt())

                val bodyBuf = buf.slice(block.offset + block.metadataLength, block.bodyLength)
                    .also { it.referenceManager.retain() }

                val msg = Message.getRootAsMessage(metadataBuf.asReadOnlyBuffer())
                val recordBatchFB = RecordBatch().also { msg.header(it) }

                (MessageSerializer.deserializeRecordBatch(recordBatchFB, bodyBuf)
                    ?: error("Failed to deserialize record batch $idx, offset ${block.offset}"))

                    .use { batch -> rel.load(batch) }

                return rel
            }
        }

        private val batches = footer.recordBatches.mapIndexed(::LoaderBatch)

        override val batchCount = batches.size

        override fun loadBatch(idx: Int, rel: Relation) = batches[idx].load(rel)
        override fun loadBatch(idx: Int, al: BufferAllocator) = loadBatch(idx, Relation(al, schema))

        override fun close() {
            buf.close()
        }
    }

    companion object {
        private fun readFooter(ch: SeekableReadChannel): ArrowFooter {
            val buf = ByteBuffer.allocate(Int.SIZE_BYTES + MAGIC.size)
            val footerLengthOffset = ch.size() - buf.remaining()
            ch.setPosition(footerLengthOffset)
            ch.readFully(buf)
            buf.flip()

            val array = buf.array()

            require(MAGIC.contentEquals(array.copyOfRange(Int.SIZE_BYTES, array.size))) {
                "missing magic number at end of Arrow file"
            }

            val footerLength = MessageSerializer.bytesToInt(array)
            require(footerLength > 0) { "Footer length must be positive" }
            require(footerLength + MAGIC.size * 2 + Int.SIZE_BYTES <= ch.size()) { "Footer length exceeds file size" }

            val footerBuffer = ByteBuffer.allocate(footerLength)
            ch.setPosition(footerLengthOffset - footerLength)
            ch.readFully(footerBuffer)
            footerBuffer.flip()
            return ArrowFooter(Footer.getRootAsFooter(footerBuffer))
        }

        @JvmStatic
        fun loader(al: BufferAllocator, ch: SeekableByteChannel): Loader {
            val readCh = SeekableReadChannel(ch)
            require(readCh.size() > MAGIC.size * 2 + 4) { "File is too small to be an Arrow file" }

            return ChannelLoader(al, readCh, readFooter(readCh))
        }

        private fun readFooter(buf: ArrowBuf): ArrowFooter {
            val magicBytes = ByteArray(Int.SIZE_BYTES + MAGIC.size)
            val footerLengthOffset = buf.capacity() - magicBytes.size
            buf.getBytes(footerLengthOffset, magicBytes)

            require(MAGIC.contentEquals(magicBytes.copyOfRange(Int.SIZE_BYTES, magicBytes.size))) {
                "missing magic number at end of Arrow file"
            }

            val footerLength = MessageSerializer.bytesToInt(magicBytes)
            require(footerLength > 0) { "Footer length must be positive" }
            require(footerLength + MAGIC.size * 2 + Int.SIZE_BYTES <= buf.capacity()) { "Footer length exceeds file size" }

            val footerBuffer = ByteBuffer.allocate(footerLength)
            buf.getBytes(footerLengthOffset - footerLength, footerBuffer)
            footerBuffer.flip()
            return ArrowFooter(Footer.getRootAsFooter(footerBuffer))
        }

        @JvmStatic
        fun loader(buf: ArrowBuf): Loader {
            buf.referenceManager.retain()
            return BufferLoader(buf, readFooter(buf))
        }

        @JvmField
        // naming from Oracle - zero cols, one row
        val DUAL = Relation(emptyList(), 1)
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

    override operator fun get(colName: String) = vectors[colName]

    @Suppress("unused")
    fun toLists(): Map<String, List<*>> {
        return vectors.mapValues { it.value.toList() }
    }

    val oldRelReader: OldRelationReader
        get() = OldRelationReader.from(vectors.sequencedValues().map(VectorReader.Companion::NewToOldAdapter), rowCount)
}
