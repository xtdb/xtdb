package xtdb.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import xtdb.api.query.IKeyFn
import xtdb.util.closeAll
import java.util.function.Function
import xtdb.arrow.RelationReader as NewRelationReader

class RelationReader private constructor(
    private val vecsMap: Map<String, IVectorReader>, override val rowCount: Int
) : NewRelationReader, AutoCloseable {

    override operator fun get(idx: Int, keyFn: IKeyFn<*>): Map<*, Any?> =
        vecsMap.values.associate { keyFn.denormalize(it.name) to it[idx] }

    override val vectors get() = vecsMap.values

    override fun vectorForOrNull(name: String) = vecsMap[name]
    override fun vectorFor(name: String) = vectorForOrNull(name) ?: error("missing vector: $name")

    override operator fun get(name: String) = vectorFor(name)

    private fun from(f: Function<IVectorReader, IVectorReader>, rowCount: Int): RelationReader =
        from(vecsMap.values.stream().map(f).toList(), rowCount)

    override fun select(idxs: IntArray): RelationReader = from({ vr -> vr.select(idxs) }, idxs.size)

    override fun select(startIdx: Int, len: Int): RelationReader = from({ vr -> vr.select(startIdx, len) }, len)

    override fun openSlice(al: BufferAllocator) = from({ vr -> vr.openSlice(al) }, rowCount)

    override fun toString(): String = "(RelationReader {rowCount=$rowCount, cols=$vecsMap})"

    override fun close() {
        vecsMap.closeAll()
    }

    companion object {
        @JvmOverloads
        @JvmStatic
        fun from(cols: List<IVectorReader>, rowCount: Int = cols.firstOrNull()?.valueCount ?: 0) =
            RelationReader(cols.associateBy { it.name }, rowCount)

        @JvmStatic
        fun from(root: VectorSchemaRoot): RelationReader =
            from(root.fieldVectors.map { v -> ValueVectorReader.from(v) }, root.rowCount)

        @JvmStatic
        fun concatCols(rel1: RelationReader, rel2: RelationReader): RelationReader {
            if (rel1.vecsMap.isEmpty()) return rel2
            if (rel2.vecsMap.isEmpty()) return rel1

            assert(rel1.rowCount == rel2.rowCount) { "Cannot concatenate relations with different row counts" }

            return from(rel1.vecsMap.values + rel2.vecsMap.values, rel1.rowCount)
        }
    }
}
