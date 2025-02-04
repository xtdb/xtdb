package xtdb.arrow

import clojure.lang.PersistentHashMap
import org.apache.arrow.flatbuf.Footer.getRootAsFooter
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorLoader
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.SeekableReadChannel
import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.*
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.api.query.IKeyFn
import xtdb.api.query.IKeyFn.KeyFn.KEBAB_CASE_KEYWORD
import xtdb.arrow.ArrowUtil.arrowBufToRecordBatch
import xtdb.arrow.Relation.UnloadMode.FILE
import xtdb.arrow.Relation.UnloadMode.STREAM
import xtdb.arrow.Vector.Companion.fromField
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.ClosedByInterruptException
import java.nio.channels.SeekableByteChannel
import java.nio.channels.WritableByteChannel
import java.util.*
import xtdb.vector.RelationReader as OldRelationReader

private val MAGIC = "ARROW1".toByteArray()

class Relation(val vectors: SequencedMap<String, Vector>, override var rowCount: Int = 0) : RelationReader {

    override val schema get() = Schema(vectors.sequencedValues().map { it.field })

    @JvmOverloads
    constructor(vectors: List<Vector>, rowCount: Int = 0)
            : this(vectors.associateByTo(linkedMapOf()) { it.name }, rowCount)

    @JvmOverloads
    constructor(allocator: BufferAllocator, schema: Schema, rowCount: Int = 0)
            : this(allocator, schema.fields, rowCount)

    @JvmOverloads
    constructor(allocator: BufferAllocator, fields: List<Field>, rowCount: Int = 0)
            : this(fields.map { fromField(allocator, it) }, rowCount)

    fun endRow() =
        (++rowCount).also { rowCount ->
            vectors.forEach { (_, vec) ->
                repeat(rowCount - vec.valueCount) { vec.writeNull() }
            }
        }

    override fun iterator() = vectors.values.iterator()

    fun rowCopier(rel: RelationReader): RowCopier {
        val copiers = rel.map { it.rowCopier(vectors[it.name] ?: error("missing ${it.name} vector")) }

        return RowCopier { srcIdx ->
            copiers.forEach { it.copyRow(srcIdx) }
            endRow()
        }
    }

    fun loadFromArrow(root: VectorSchemaRoot) {
        vectors.forEach { (name, vec) -> vec.loadFromArrow(root.getVector(name)) }
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

        vectors.values.forEach { it.unloadPage(nodes, buffers) }

        return ArrowRecordBatch(rowCount, nodes, buffers)
    }

    fun openAsRoot(al: BufferAllocator): VectorSchemaRoot =
        VectorSchemaRoot.create(Schema(vectors.values.map { it.field }), al)
            .also { vsr ->
                openArrowRecordBatch().use { recordBatch ->
                    VectorLoader(vsr).load(recordBatch)
                }
            }

    inner class RelationUnloader(private val ch: WriteChannel, private val mode: UnloadMode) : AutoCloseable {

        private val schema = Schema(this@Relation.vectors.values.map { it.field })
        private val arrowBlocks = mutableListOf<ArrowBlock>()

        init {
            try {
                mode.start(ch)
                MessageSerializer.serialize(ch, schema)
            } catch (_: ClosedByInterruptException) {
                throw InterruptedException()
            }
        }

        fun writePage() {
            try {
                openArrowRecordBatch().use { recordBatch ->
                    MessageSerializer.serialize(ch, recordBatch)
                        .also { arrowBlocks.add(it) }
                }
            } catch (_: ClosedByInterruptException) {
                throw InterruptedException()
            }
        }

        fun end() {
            mode.end(ch, schema, arrowBlocks)
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
            }

            return ByteBuffer.wrap(baos.toByteArray())
        }

    private fun load(recordBatch: ArrowRecordBatch) {
        val nodes = recordBatch.nodes.toMutableList()
        val buffers = recordBatch.buffers.toMutableList()
        vectors.values.forEach { it.loadPage(nodes, buffers) }

        require(nodes.isEmpty()) { "Unconsumed nodes: $nodes" }
        require(buffers.isEmpty()) { "Unconsumed buffers: $buffers" }

        rowCount = recordBatch.length
    }

    sealed class Loader : AutoCloseable {
        protected interface Page {
            fun load(rel: Relation)
        }

        abstract val schema: Schema
        protected abstract val pages: List<Page>
        val pageCount get() = pages.size

        private var lastPageIndex = -1

        fun loadPage(idx: Int, al: BufferAllocator) = Relation(al, schema).also { loadPage(idx, it) }

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
        private val ch: SeekableReadChannel,
        footer: ArrowFooter
    ) : Loader() {
        inner class Page(private val idx: Int, private val arrowBlock: ArrowBlock) : Loader.Page {
            override fun load(rel: Relation) {
                ch.setPosition(arrowBlock.offset)

                (MessageSerializer.deserializeRecordBatch(ch, arrowBlock, al)
                    ?: error("Failed to deserialize record batch $idx, offset ${arrowBlock.offset}"))

                    .use { rel.load(it) }
            }
        }

        override val schema: Schema = footer.schema
        override val pages = footer.recordBatches.mapIndexed(::Page)

        override fun close() = ch.close()
    }

    private class BufferLoader(
        private val buf: ArrowBuf,
        footer: ArrowFooter
    ) : Loader() {
        override val schema: Schema = footer.schema

        inner class Page(private val idx: Int, private val arrowBlock: ArrowBlock) : Loader.Page {

            override fun load(rel: Relation) {
                buf.arrowBufToRecordBatch(
                    arrowBlock.offset, arrowBlock.metadataLength, arrowBlock.bodyLength, "Failed to deserialize record batch $idx, offset ${arrowBlock.offset}"
                ).use { rel.load(it) }
            }
        }

        override val pages = footer.recordBatches.mapIndexed(::Page)

        override fun close() = buf.close()
    }

    companion object {
        @JvmStatic
        fun readFooter(ch: SeekableReadChannel): ArrowFooter {
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
            return ArrowFooter(getRootAsFooter(footerBuffer))
        }

        @JvmStatic
        fun loader(al: BufferAllocator, ch: SeekableByteChannel): Loader {
            val readCh = SeekableReadChannel(ch)
            require(readCh.size() > MAGIC.size * 2 + 4) { "File is too small to be an Arrow file" }

            return ChannelLoader(al, readCh, readFooter(readCh))
        }

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

        @Suppress("unused")
        @JvmField
        // naming from Oracle - zero cols, one row
        val DUAL = Relation(emptyList(), 1)

        @JvmStatic
        fun fromRoot(vsr: VectorSchemaRoot) = Relation(vsr.fieldVectors.map(Vector::fromArrow), vsr.rowCount)

        @JvmStatic
        fun fromRecordBatch(allocator: BufferAllocator, schema: Schema, recordBatch: ArrowRecordBatch): Relation {
            val rel = Relation(allocator, schema)
            // this load retains the buffers
            rel.load(recordBatch)
            return rel
        }
    }

    /**
     * Resets the row count and all vectors, leaving the buffers allocated.
     */
    fun clear() {
        vectors.forEach { (_, vec) -> vec.clear() }
        rowCount = 0
    }

    override fun close() {
        vectors.forEach { (_, vec) -> vec.close() }
    }

    override operator fun get(colName: String) = vectors[colName]

    @Suppress("unused")
    @JvmOverloads
    fun toTuples(keyFn: IKeyFn<*> = KEBAB_CASE_KEYWORD) =
        (0..<rowCount).map { idx -> vectors.map { it.value.getObject(idx, keyFn) } }

    @Suppress("unused")
    @JvmOverloads
    fun toMaps(keyFn: IKeyFn<*> = KEBAB_CASE_KEYWORD) =
        (0..<rowCount).map { idx ->
            PersistentHashMap.create(
                vectors.entries.associate {
                    Pair(
                        keyFn.denormalize(it.key),
                        it.value.getObject(idx, keyFn)
                    )
                }
            ) as Map<*, *>
        }

    val oldRelReader: OldRelationReader
        get() = OldRelationReader.from(vectors.sequencedValues().map(VectorReader.Companion::NewToOldAdapter), rowCount)
}
