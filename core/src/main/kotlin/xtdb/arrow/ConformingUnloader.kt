package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import xtdb.api.error.Fault
import xtdb.arrow.Vector.Companion.openVector
import xtdb.util.closeAll

/**
 * Builds one record batch in the layout of a target schema from a page whose vectors may lack some of its struct fields
 * or union legs, hold them in another order, or hold a single type where the target has a union.
 *
 * Vectors are matched to the target by name and unload their own buffers wherever the shapes agree;
 * only what a page lacks, or can't be re-laid without rewriting values, is built here, and released once the batch holds it.
 */
internal class ConformingUnloader(private val al: BufferAllocator) : AutoCloseable {
    val nodes = mutableListOf<ArrowFieldNode>()
    val buffers = mutableListOf<ArrowBuf>()
    private val generated = mutableListOf<AutoCloseable>()

    private fun <C : AutoCloseable> generated(c: C) = c.also { generated += it }

    fun unload(vec: VectorReader, target: Field) =
        when {
            vec !is Vector -> copy(vec, target)
            target.type is ArrowType.Union && vec !is DenseUnionVector -> wrapInUnion(vec, target)
            else -> vec.unloadPage(target, this)
        }

    fun copy(src: VectorReader, target: Field) {
        val out = generated(al.openVector(target))
        out.append(src)
        out.unloadPage(nodes, buffers)
    }

    fun nulls(target: Field, valueCount: Int) {
        val canBeNull = when (target.type) {
            is ArrowType.Union -> target.children.any { it.type == NULL_TYPE }
            else -> target.isNullable || target.type == NULL_TYPE
        }

        if (!canBeNull) throw Fault(
            "a page has no '${target.name}', and its declared type ${target.type} can't hold the nulls that stand in for it",
            "xtdb/page-missing-non-nullable"
        )

        val out = generated(al.openVector(target))
        repeat(valueCount) { out.writeNull() }
        out.unloadPage(nodes, buffers)
    }

    fun empty(target: Field) = generated(al.openVector(target)).unloadPage(nodes, buffers)

    fun notInTarget(vec: VectorReader, target: Any): Nothing =
        throw Fault("a page's '${vec.name}' (${vec.arrowType}) isn't in its declared type $target", "xtdb/page-outside-declared-type")

    private fun unloadBuffer(len: Int, write: ExtensibleBuffer.(Int) -> Unit) =
        generated(ExtensibleBuffer(al)).apply { repeat(len) { write(it) } }.unloadBuffer(buffers)

    fun unloadTypeIds(len: Int, typeId: (Int) -> Byte) = unloadBuffer(len) { writeByte(typeId(it)) }

    private fun wrapInUnion(vec: Vector, target: Field) {
        val legIdx = target.children.indexOfFirst { it.name == vec.arrowType.toLeg() }

        // a null in a single-typed vector belongs in the union's null leg, which would mean rewriting its values
        if (legIdx < 0 || (vec.nullable && (0 until vec.valueCount).any(vec::isNull))) return copy(vec, target)

        nodes.add(ArrowFieldNode(vec.valueCount.toLong(), -1))
        unloadTypeIds(vec.valueCount) { legIdx.toByte() }
        unloadBuffer(vec.valueCount) { writeInt(it) }

        for ((idx, leg) in target.children.withIndex())
            if (idx == legIdx) unload(vec, leg) else empty(leg)
    }

    fun openRecordBatch(rowCount: Int) = ArrowRecordBatch(rowCount, nodes, buffers)

    override fun close() = generated.closeAll()
}
