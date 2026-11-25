package xtdb.arrow

import clojure.lang.*
import com.google.protobuf.ByteString
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.InternalApi
import xtdb.api.query.IKeyFn
import xtdb.api.query.IKeyFn.KeyFn.KEBAB_CASE_KEYWORD
import xtdb.util.closeAll
import xtdb.util.closeAllOnCatch
import xtdb.util.closeOnCatch
import xtdb.util.safeMap
import xtdb.util.safeMapValues
import java.util.*

interface RelationReader : ILookup, Seqable, Counted, AutoCloseable {
    val schema: Schema get() = Schema(cols.map { (name, v) -> v.toField(name) })
    val rowCount: Int

    /** This relation's columns, in order, keyed by their names in it. */
    val cols: Map<String, VectorReader>

    fun vectorForOrNull(name: String): VectorReader?
    fun vectorFor(name: String) = vectorForOrNull(name) ?: error("missing vector: $name")
    operator fun get(name: String) = vectorFor(name)

    operator fun get(idx: Int, keyFn: IKeyFn<*> = KEBAB_CASE_KEYWORD): Map<*, Any?> =
        cols.entries.associate { (name, v) -> keyFn.denormalize(name) to v.getObject(idx, keyFn) }

    /**
     * The rows `[startIdx, startIdx + len)` as a one-page Arrow IPC stream, carrying this relation's whole
     * schema, [len] defaulting to the rest of the relation.
     *
     * The bytes are copied out of this relation's memory, so the result may outlive it.
     */
    @OptIn(InternalApi::class)
    fun toArrowStream(startIdx: Int = 0, len: Int = rowCount - startIdx): ByteString =
        PageOutput()
            .also { out -> for (v in cols.values) v.write(out, startIdx, len) }
            .toArrowStream(schema, len)

    fun openSlice(al: BufferAllocator): RelationReader =
        cols.values
            .safeMap { it.openSlice(al) }
            .closeAllOnCatch { sliced -> from(cols.keys.zip(sliced).toMap(linkedMapOf()), rowCount) }

    /**
     * This page as one record batch in the layout of [schema] - its struct fields and union legs, in its order -
     * reusing the page's own buffers wherever they already fit; see [ConformingUnloader].
     * Columns the page doesn't have are all-null.
     */
    fun openArrowRecordBatch(al: BufferAllocator, schema: Schema): ArrowRecordBatch =
        ConformingUnloader(al).use { unloader ->
            val targetCols = schema.fields.associateBy { it.name }
            cols.entries.firstOrNull { it.key !in targetCols }?.let { unloader.notInTarget(it.key, it.value, schema) }

            for (field in schema.fields)
                vectorForOrNull(field.name)?.let { unloader.unload(it, field) } ?: unloader.nulls(field, rowCount)

            unloader.openRecordBatch(rowCount)
        }

    fun openDirectSlice(al: BufferAllocator) =
        cols.safeMapValues { _, v -> v.openDirectSlice(al) }
            .closeAllOnCatch { slices -> Relation(al, LinkedHashMap(slices), rowCount) }

    fun select(idxs: IntArray): RelationReader = from(cols.mapValuesTo(linkedMapOf()) { it.value.select(idxs) }, idxs.size)

    fun rowCopier(dest: RelationWriter): RowCopier {
        val colNames = cols.keys + dest.cols.keys

        val copiers = colNames.map { colName ->
            val srcVec = vectorForOrNull(colName) ?: NullVector(true, rowCount)
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

    override fun close() = cols.values.closeAll()

    @Suppress("unused") // was used in XT flight-sql last I checked
    fun toTuples() = toTuples(KEBAB_CASE_KEYWORD)

    fun toTuples(keyFn: IKeyFn<*> = KEBAB_CASE_KEYWORD) =
        List(rowCount) { idx -> cols.values.map { it.getObject(idx, keyFn) } }

    val asMaps get() = toMaps(KEBAB_CASE_KEYWORD)

    fun <K> toMaps(keyFn: IKeyFn<K>): List<Map<K, *>> =
        List(rowCount) { idx ->
            @Suppress("UNCHECKED_CAST")
            PersistentHashMap.create(
                cols.entries
                    .associate { (name, v) ->
                        Pair(
                            keyFn.denormalize(name),
                            v.getObject(idx, keyFn)
                        )
                    }
                    .filterValues { it != null }
            ) as Map<K, *>
        }

    private class FromCols(
        override val cols: SequencedMap<String, VectorReader>, override val rowCount: Int
    ) : RelationReader {
        override fun vectorForOrNull(name: String) = cols[name]
    }

    companion object {
        fun from(cols: Map<String, VectorReader>): RelationReader =
            FromCols(LinkedHashMap(cols), cols.values.firstOrNull()?.valueCount ?: 0)

        @JvmStatic
        fun from(cols: Map<String, VectorReader>, rowCount: Int): RelationReader =
            FromCols(LinkedHashMap(cols), rowCount)

        @JvmStatic
        fun concatCols(rel1: RelationReader, rel2: RelationReader): RelationReader {
            if (rel1.cols.isEmpty()) return rel2
            if (rel2.cols.isEmpty()) return rel1
            assert(rel1.rowCount == rel2.rowCount) { "Cannot concatenate relations with different row counts" }

            return from(rel1.cols + rel2.cols, rel1.rowCount)
        }

        @Suppress("unused")
        @JvmField
        // naming from Oracle - zero cols, one row
        val DUAL = from(emptyMap(), 1)
    }

    override fun valAt(key: Any?) = valAt(key, null)
    override fun valAt(key: Any?, notFound: Any?) = vectorForOrNull(key as String) ?: notFound

    override fun seq(): ISeq? = RT.seq(cols.values)
    override fun count() = cols.size
}

/**
 * This relation as one struct column: the struct owns [RelationReader.openDirectSlice]s of the columns
 * (zero-copy for a [Vector], a copy for a selection), so it outlives this relation.
 */
internal fun RelationReader.openAsStruct(al: BufferAllocator): StructVector =
    openDirectSlice(al).closeOnCatch { slices -> StructVector(al, false, LinkedHashMap(slices.vecs), rowCount) }
