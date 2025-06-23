package xtdb.arrow

import clojure.lang.Keyword
import clojure.lang.Symbol
import org.apache.arrow.flatbuf.Footer.getRootAsFooter
import org.apache.arrow.flatbuf.MessageHeader
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorLoader
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.SeekableReadChannel
import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.*
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.ArrowWriter
import xtdb.arrow.ArrowUtil.arrowBufToRecordBatch
import xtdb.arrow.Relation.UnloadMode.FILE
import xtdb.arrow.Relation.UnloadMode.STREAM
import xtdb.arrow.Vector.Companion.fromField
import xtdb.toFieldType
import xtdb.trie.FileSize
import xtdb.types.NamelessField
import xtdb.util.closeAll
import xtdb.util.closeAllOnCatch
import xtdb.util.closeOnCatch
import xtdb.util.normalForm
import xtdb.util.safeMap
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.*
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ
import java.util.*

private val MAGIC = "ARROW1".toByteArray()

class Relation(
    private val al: BufferAllocator, val vecs: SequencedMap<String, Vector>, override var rowCount: Int
) : RelationWriter {

    override val schema get() = Schema(vecs.sequencedValues().map { it.field })
    override val vectors get() = vecs.values

    constructor(al: BufferAllocator) : this(al, linkedMapOf<String, Vector>(), 0)

    constructor(al: BufferAllocator, vectors: List<Vector>, rowCount: Int)
            : this(al, vectors.associateByTo(linkedMapOf()) { it.name }, rowCount)

    override fun vectorForOrNull(name: String) = vecs[name]
    override fun vectorFor(name: String) = vectorForOrNull(name) ?: error("missing vector: $name")
    override operator fun get(name: String) = vectorFor(name)

    override fun vectorFor(name: String, fieldType: FieldType): Vector =
        vecs.compute(name) { _, v ->
            v?.maybePromote(al, fieldType)
                ?: fromField(al, Field(name, fieldType, null))
                    .also { vec -> repeat(rowCount) { vec.writeNull() } }
        }!!

    override fun endRow() =
        (++rowCount).also { rowCount ->
            vecs.forEach { (_, vec) ->
                repeat(rowCount - vec.valueCount) { vec.writeNull() }
            }
        }

    fun loadFromArrow(root: VectorSchemaRoot) {
        vecs.forEach { (name, vec) -> vec.loadFromArrow(root.getVector(name)) }
        rowCount = root.rowCount
    }

    enum class UnloadMode {
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

    internal fun openArrowRecordBatch(): ArrowRecordBatch {
        val nodes = mutableListOf<ArrowFieldNode>()
        val buffers = mutableListOf<ArrowBuf>()

        vecs.values.forEach { it.unloadPage(nodes, buffers) }

        return ArrowRecordBatch(rowCount, nodes, buffers)
    }

    fun openAsRoot(al: BufferAllocator): VectorSchemaRoot =
        VectorSchemaRoot.create(Schema(vecs.values.map { it.field }), al)
            .also { vsr ->
                openArrowRecordBatch().use { recordBatch ->
                    VectorLoader(vsr).load(recordBatch)
                }
            }

    inner class RelationUnloader(private val ch: WriteChannel, private val mode: UnloadMode) : ArrowWriter {

        private val schema = Schema(this@Relation.vecs.values.map { it.field })
        private val arrowBlocks = mutableListOf<ArrowBlock>()

        init {
            try {
                mode.start(ch)
                MessageSerializer.serialize(ch, schema)
            } catch (_: ClosedByInterruptException) {
                throw InterruptedException()
            }
        }

        override fun writePage() {
            try {
                openArrowRecordBatch().use { recordBatch ->
                    MessageSerializer.serialize(ch, recordBatch)
                        .also { arrowBlocks.add(it) }
                }
            } catch (_: ClosedByInterruptException) {
                throw InterruptedException()
            }
        }

        override fun end(): FileSize {
            mode.end(ch, schema, arrowBlocks)
            return ch.currentPosition
        }

        override fun close() {
            ch.close()
        }
    }

    @JvmOverloads
    fun startUnload(ch: WritableByteChannel, mode: UnloadMode = FILE) =
        RelationUnloader(WriteChannel(ch), mode)

    val asArrowStream: ByteBuffer
        get() {
            val baos = ByteArrayOutputStream()
            startUnload(Channels.newChannel(baos), STREAM).use { unl ->
                unl.writePage()
                unl.end()
            }

            return ByteBuffer.wrap(baos.toByteArray())
        }

    private fun load(recordBatch: ArrowRecordBatch) {
        val nodes = recordBatch.nodes.toMutableList()
        val buffers = recordBatch.buffers.toMutableList()
        vecs.values.forEach { it.loadPage(nodes, buffers) }
        require(nodes.isEmpty()) { "Unconsumed nodes: $nodes" }
        require(buffers.isEmpty()) { "Unconsumed buffers: $buffers" }

        rowCount = recordBatch.length
    }

    class StreamLoader(al: BufferAllocator, ch: ReadableByteChannel) : AutoCloseable {

        private val reader = MessageChannelReader(ReadChannel(ch), al)

        val schema: Schema

        init {
            val schemaMessage = (reader.readNext() ?: error("empty stream")).message
            check(schemaMessage.headerType() == MessageHeader.Schema) { "expected schema message" }

            schema = MessageSerializer.deserializeSchema(schemaMessage)
        }

        fun loadNextPage(rel: Relation): Boolean {
            val msg = reader.readNext() ?: return false

            msg.message.headerType().let {
                check(it == MessageHeader.RecordBatch) { "unexpected Arrow message type: $it" }
            }

            MessageSerializer.deserializeRecordBatch(msg.message, msg.bodyBuffer)
                .use { rel.load(it) }

            return true
        }

        override fun close() = reader.close()
    }

    sealed class Loader : AutoCloseable {
        protected interface Page {
            fun load(rel: Relation)
        }

        abstract val schema: Schema
        protected abstract val pages: List<Page>
        val pageCount get() = pages.size

        private var lastPageIndex = -1

        fun loadPage(idx: Int, al: BufferAllocator) = open(al, schema).also { loadPage(idx, it) }

        fun loadPage(idx: Int, rel: Relation) {
            pages[idx].load(rel)
            lastPageIndex = idx
        }

        fun loadNextPage(rel: Relation): Boolean {
            if (lastPageIndex + 1 >= pageCount) return false

            loadPage(++lastPageIndex, rel)
            return true
        }
    }

    private class ChannelLoader(
        private val al: BufferAllocator,
        ch: SeekableByteChannel,
        footer: ArrowFooter
    ) : Loader() {
        val arrowCh = SeekableReadChannel(ch)

        inner class Page(private val idx: Int, private val arrowBlock: ArrowBlock) : Loader.Page {
            override fun load(rel: Relation) {
                arrowCh.setPosition(arrowBlock.offset)

                (MessageSerializer.deserializeRecordBatch(arrowCh, arrowBlock, al)
                    ?: error("Failed to deserialize record batch $idx, offset ${arrowBlock.offset}"))

                    .use { rel.load(it) }
            }
        }

        override val schema: Schema = footer.schema
        override val pages = footer.recordBatches.mapIndexed(::Page)

        override fun close() = arrowCh.close()
    }

    private class BufferLoader(private val buf: ArrowBuf, footer: ArrowFooter) : Loader() {
        override val schema: Schema = footer.schema

        inner class Page(private val idx: Int, private val arrowBlock: ArrowBlock) : Loader.Page {

            override fun load(rel: Relation) {
                buf.arrowBufToRecordBatch(
                    arrowBlock.offset, arrowBlock.metadataLength, arrowBlock.bodyLength,
                    "Failed to deserialize record batch $idx, offset ${arrowBlock.offset}"
                ).use { rel.load(it) }
            }
        }

        override val pages = footer.recordBatches.mapIndexed(::Page)

        override fun close() = buf.close()
    }

    companion object {
        @JvmStatic
        fun readFooter(ch: SeekableByteChannel): ArrowFooter {
            val buf = ByteBuffer.allocate(Int.SIZE_BYTES + MAGIC.size)
            val footerLengthOffset = ch.size() - buf.remaining()
            ch.position(footerLengthOffset)
            ch.read(buf)
            buf.flip()

            val array = buf.array()

            require(MAGIC.contentEquals(array.copyOfRange(Int.SIZE_BYTES, array.size))) {
                "missing magic number at end of Arrow file"
            }

            val footerLength = MessageSerializer.bytesToInt(array)
            require(footerLength > 0) { "Footer length must be positive" }
            require(footerLength + MAGIC.size * 2 + Int.SIZE_BYTES <= ch.size()) { "Footer length exceeds file size" }

            val footerBuffer = ByteBuffer.allocate(footerLength)
            ch.position(footerLengthOffset - footerLength)
            ch.read(footerBuffer)
            footerBuffer.flip()
            return ArrowFooter(getRootAsFooter(footerBuffer))
        }

        @JvmStatic
        fun loader(al: BufferAllocator, ch: SeekableByteChannel): Loader {
            require(ch.size() > MAGIC.size * 2 + 4) { "File is too small to be an Arrow file" }

            return ChannelLoader(al, ch, readFooter(ch))
        }

        @JvmStatic
        fun loader(al: BufferAllocator, path: Path): Loader = loader(al, Files.newByteChannel(path, READ))

        @JvmStatic
        fun readFooter(buf: ArrowBuf): ArrowFooter {
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
            return ArrowFooter(getRootAsFooter(footerBuffer))
        }

        @JvmStatic
        fun loader(buf: ArrowBuf): Loader {
            buf.referenceManager.retain()

            try {
                return BufferLoader(buf, readFooter(buf))
            } catch (e: Throwable) {
                buf.close()
                throw e
            }
        }

        @JvmStatic
        fun fromRoot(al: BufferAllocator, vsr: VectorSchemaRoot) =
            Relation(al, vsr.fieldVectors.map(Vector::fromArrow), vsr.rowCount)

        @JvmStatic
        fun fromRecordBatch(allocator: BufferAllocator, schema: Schema, recordBatch: ArrowRecordBatch): Relation {
            val rel = open(allocator, schema)
            // this load retains the buffers
            rel.load(recordBatch)
            return rel
        }

        @JvmStatic
        fun open(al: BufferAllocator, schema: Schema) = open(al, schema.fields)

        @JvmStatic
        fun open(al: BufferAllocator, fields: List<Field>) =
            Relation(al, fields.map { fromField(al, it) }, 0)

        @JvmStatic
        fun open(al: BufferAllocator, fields: SequencedMap<String, NamelessField>) =
            open(al, fields.map { (name, field) -> field.toArrowField(name) })

        @JvmStatic
        fun openFromRows(al: BufferAllocator, rows: List<Map<*, *>>): Relation =
            Relation(al).closeOnCatch { rel -> rel.also { for (row in rows) it.writeRow(row) } }

        @JvmStatic
        fun openFromCols(al: BufferAllocator, cols: Map<*, List<*>>): Relation =
            cols.entries.safeMap { col ->
                val normalKey = when (val k = col.key) {
                    is String -> k
                    is Symbol -> normalForm(k).toString()
                    is Keyword -> normalForm(k.sym).toString()
                    else -> throw IllegalArgumentException("Column name must be a string, keyword or symbol")
                }

                Vector.fromList(al,normalKey, col.value)
            }.closeAllOnCatch { Relation(al, it, it.firstOrNull()?.valueCount ?: 0) }
    }

    /**
     * Resets the row count and all vectors, leaving the buffers allocated.
     */
    override fun clear() {
        vecs.forEach { (_, vec) -> vec.clear() }
        rowCount = 0
    }

    override fun close() {
        vecs.closeAll()
        rowCount = 0
    }
}
