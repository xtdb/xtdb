package xtdb.arrow

import clojure.lang.Keyword
import clojure.lang.Symbol
import org.apache.arrow.flatbuf.MessageHeader
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorLoader
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.*
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.ArrowWriter
import xtdb.arrow.ArrowUnloader.Mode
import xtdb.arrow.ArrowUnloader.Mode.FILE
import xtdb.arrow.ArrowUnloader.Mode.STREAM
import xtdb.arrow.Vector.Companion.fromField
import xtdb.types.NamelessField
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

class Relation(
    private val al: BufferAllocator, val vecs: SequencedMap<String, Vector>, override var rowCount: Int
) : RelationWriter {

    override val schema get() = Schema(vecs.sequencedValues().map { it.field })
    override val vectors get() = vecs.values

    constructor(al: BufferAllocator) : this(al, linkedMapOf<String, Vector>(), 0)

    constructor(al: BufferAllocator, vectors: List<Vector>, rowCount: Int)
            : this(al, vectors.associateByTo(linkedMapOf()) { it.name }, rowCount)

    constructor(al: BufferAllocator, schema: Schema)
            : this(al, schema.fields.safeMap { fromField(al, it) }, 0)

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

    internal fun openArrowRecordBatch(): ArrowRecordBatch {
        val nodes = mutableListOf<ArrowFieldNode>()
        val buffers = mutableListOf<ArrowBuf>()

        vecs.values.forEach { it.unloadPage(nodes, buffers) }

        return ArrowRecordBatch(rowCount, nodes, buffers)
    }

    fun openAsRoot(al: BufferAllocator): VectorSchemaRoot =
        VectorSchemaRoot.create(schema, al)
            .also { vsr ->
                openArrowRecordBatch().use { recordBatch ->
                    VectorLoader(vsr).load(recordBatch)
                }
            }

    inner class RelationUnloader(private val arrowUnloader: ArrowUnloader) : ArrowWriter {

        override fun writePage() {
            try {
                openArrowRecordBatch().use { arrowUnloader.writeBatch(it) }
            } catch (_: ClosedByInterruptException) {
                throw InterruptedException()
            }
        }

        override fun end() = arrowUnloader.end()

        override fun close() = arrowUnloader.close()
    }

    @JvmOverloads
    fun startUnload(ch: WritableByteChannel, mode: Mode = FILE) =
        RelationUnloader(ArrowUnloader.open(ch, schema, mode))

    val asArrowStream: ByteBuffer
        get() {
            val baos = ByteArrayOutputStream()
            startUnload(Channels.newChannel(baos), STREAM).use { unl ->
                unl.writePage()
                unl.end()
            }

            return ByteBuffer.wrap(baos.toByteArray())
        }

    fun load(recordBatch: ArrowRecordBatch) {
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

    class Loader(private val arrowFileLoader: ArrowFileLoader) : AutoCloseable {

        val schema: Schema get() = arrowFileLoader.schema
        val pageCount get() = arrowFileLoader.pageCount

        private var lastPageIndex = -1

        fun loadPage(idx: Int, al: BufferAllocator) = open(al, schema).closeOnCatch { loadPage(idx, it); it }

        fun loadPage(idx: Int, rel: Relation) {
            arrowFileLoader.openPage(idx).use { rel.load(it) }
            lastPageIndex = idx
        }

        fun loadNextPage(rel: Relation): Boolean {
            if (lastPageIndex + 1 >= pageCount) return false

            loadPage(++lastPageIndex, rel)
            return true
        }

        override fun close() = arrowFileLoader.close()
    }

    companion object {

        @JvmStatic
        fun loader(al: BufferAllocator, ch: SeekableByteChannel): Loader =
            Loader(ArrowFileLoader.openFromChannel(al, ch))

        @JvmStatic
        fun loader(al: BufferAllocator, bytes: ByteArray) = loader(al, bytes.asChannel)

        @JvmStatic
        fun loader(al: BufferAllocator, path: Path) = loader(al, Files.newByteChannel(path, READ))

        @JvmStatic
        fun loader(buf: ArrowBuf) = Loader(ArrowFileLoader.openFromArrowBuf(buf))

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
        @Suppress("unused") // util if we ever need it
        fun openFromCols(al: BufferAllocator, cols: Map<*, List<*>>): Relation =
            cols.entries.safeMap { col ->
                val normalKey = when (val k = col.key) {
                    is String -> k
                    is Symbol -> normalForm(k).toString()
                    is Keyword -> normalForm(k.sym).toString()
                    else -> throw IllegalArgumentException("Column name must be a string, keyword or symbol")
                }

                Vector.fromList(al, normalKey, col.value)
            }.closeAllOnCatch { Relation(al, it, it.firstOrNull()?.valueCount ?: 0) }
    }
}
