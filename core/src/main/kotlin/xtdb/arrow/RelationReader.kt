package xtdb.arrow

import clojure.lang.*
import com.google.protobuf.ByteString
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.compression.NoCompressionCodec
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.InternalApi
import xtdb.api.query.IKeyFn
import xtdb.api.query.IKeyFn.KeyFn.KEBAB_CASE_KEYWORD
import xtdb.util.closeAll
import xtdb.util.closeAllOnCatch
import xtdb.util.safeMap
import java.util.*

interface RelationReader : ILookup, Seqable, Counted, AutoCloseable {
    val schema: Schema get() = Schema(vectors.map { it.field })
    val rowCount: Int

    val vectors: Collection<VectorReader>

    fun vectorForOrNull(name: String): VectorReader?
    fun vectorFor(name: String) = vectorForOrNull(name) ?: error("missing vector: $name")
    operator fun get(name: String) = vectorFor(name)

    operator fun get(idx: Int, keyFn: IKeyFn<*> = KEBAB_CASE_KEYWORD): Map<*, Any?> =
        vectors.associate { keyFn.denormalize(it.name) to it.getObject(idx, keyFn) }

    // Kotlin's parameter defaults are call-site substitutions, so they generate no JVM overload, and
    // `@JvmOverloads` is rejected on an interface member. Clojure can only reach the no-arg form if it
    // exists in its own right - `xtdb.flight-sql-test` calls it.
    fun openArrowRecordBatch(): ArrowRecordBatch = openArrowRecordBatch(0)

    /**
     * Opens a record batch over the rows `[startIdx, startIdx + len)`, sharing this relation's memory
     * rather than copying it, [len] defaulting to the rest of the relation.
     *
     * Every buffer in the batch carries exactly one reference, which closing the batch releases:
     * [VectorReader.unloadPage] retains a vector's own buffers on the way in, so the batch is built with
     * `retainBuffers = false` and a buffer the unload had to allocate — rebased offsets, unaligned
     * validity — needs no other owner.
     *
     * The batch may therefore outlive the relation, so long as it is closed.
     */
    @OptIn(InternalApi::class)
    fun openArrowRecordBatch(startIdx: Int = 0, len: Int = rowCount - startIdx): ArrowRecordBatch {
        val nodes = mutableListOf<ArrowFieldNode>()

        // The list owns each reference from the moment `unloadPage` puts it there until the batch takes
        // them all, so a vector part-way through the fan-out throwing — a row range a vector can't serve,
        // an allocation that fails — releases what its predecessors retained rather than stranding it.
        return mutableListOf<ArrowBuf>().closeAllOnCatch { buffers ->
            for (v in vectors) v.unloadPage(nodes, buffers, startIdx, len)

            ArrowRecordBatch(
                len, nodes, buffers, NoCompressionCodec.DEFAULT_BODY_COMPRESSION,
                /* alignBuffers = */ true, /* retainBuffers = */ false
            )
        }
    }

    /**
     * The rows `[startIdx, startIdx + len)` as a one-page Arrow IPC stream, carrying this relation's whole
     * schema, [len] defaulting to the rest of the relation.
     *
     * The bytes are copied out of this relation's memory, so the result may outlive it.
     */
    @OptIn(InternalApi::class)
    fun toArrowStream(startIdx: Int = 0, len: Int = rowCount - startIdx): ByteString =
        PageOutput()
            .also { out -> for (v in vectors) v.write(out, startIdx, len) }
            .toArrowStream(schema, len)

    fun openSlice(al: BufferAllocator): RelationReader =
        vectors
            .safeMap { it.openSlice(al) }
            .closeAllOnCatch { slicedVecs -> from(slicedVecs, rowCount) }

    fun openDirectSlice(al: BufferAllocator) =
        vectors
            .safeMap { it.openDirectSlice(al) }
            .closeAllOnCatch { vectors -> Relation(al, vectors, rowCount) }

    fun select(idxs: IntArray): RelationReader = from(vectors.map { it.select(idxs) }, idxs.size)
    fun select(startIdx: Int, len: Int): RelationReader = from(vectors.map { it.select(startIdx, len) }, len)

    fun rowCopier(dest: RelationWriter): RowCopier {
        val colNames = vectors.mapTo(mutableSetOf()) { it.name } + dest.vectors.map { it.name }

        val copiers = colNames.map { colName ->
            val srcVec = vectorForOrNull(colName) ?: NullVector(colName, true, rowCount)
            srcVec.rowCopier(dest.vectorForOrNull(colName) ?: dest.vectorFor(colName, srcVec.arrowType, srcVec.nullable))
        }

        return object : RowCopier {
            override fun copyRow(srcIdx: Int) {
                copiers.forEach { it.copyRow(srcIdx) }
                dest.rowCount++
            }

            override fun copyRows(sel: IntArray) {
                copiers.forEach { it.copyRows(sel) }
                dest.rowCount += sel.size
            }

            override fun copyRange(startIdx: Int, len: Int) {
                copiers.forEach { it.copyRange(startIdx, len) }
                dest.rowCount += len
            }
        }
    }

    override fun close() = vectors.closeAll()

    @Suppress("unused") // was used in XT flight-sql last I checked
    fun toTuples() = toTuples(KEBAB_CASE_KEYWORD)

    fun toTuples(keyFn: IKeyFn<*> = KEBAB_CASE_KEYWORD) =
        List(rowCount) { idx -> vectors.map { it.getObject(idx, keyFn) } }

    val asMaps get() = toMaps(KEBAB_CASE_KEYWORD)

    fun <K> toMaps(keyFn: IKeyFn<K>): List<Map<K, *>> =
        List(rowCount) { idx ->
            @Suppress("UNCHECKED_CAST")
            PersistentHashMap.create(
                vectors
                    .associate {
                        Pair(
                            keyFn.denormalize(it.name),
                            it.getObject(idx, keyFn)
                        )
                    }
                    .filterValues { it != null }
            ) as Map<K, *>
        }

    private class FromCols(
        private val cols: SequencedMap<String, VectorReader>, override val rowCount: Int
    ) : RelationReader {
        override fun vectorForOrNull(name: String) = cols[name]
        override val vectors get() = cols.values
    }

    companion object {
        fun from(cols: List<VectorReader>): RelationReader =
            FromCols(cols.associateByTo(linkedMapOf()) { it.name }, cols.firstOrNull()?.valueCount ?: 0)

        @JvmStatic
        fun from(cols: Iterable<VectorReader>, rowCount: Int): RelationReader =
            FromCols(cols.associateByTo(linkedMapOf()) { it.name }, rowCount)

        @JvmStatic
        fun concatCols(rel1: RelationReader, rel2: RelationReader): RelationReader {
            if (rel1.vectors.isEmpty()) return rel2
            if (rel2.vectors.isEmpty()) return rel1
            assert(rel1.rowCount == rel2.rowCount) { "Cannot concatenate relations with different row counts" }

            return from(rel1.vectors + rel2.vectors, rel1.rowCount)
        }

        @Suppress("unused")
        @JvmField
        // naming from Oracle - zero cols, one row
        val DUAL = from(emptyList(), 1)
    }

    override fun valAt(key: Any?) = valAt(key, null)
    override fun valAt(key: Any?, notFound: Any?) = vectorForOrNull(key as String) ?: notFound

    override fun seq(): ISeq? = RT.seq(vectors)
    override fun count() = vectors.count()
}
