package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.VectorType.*
import xtdb.arrow.agg.VectorSummer
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher
import org.apache.arrow.vector.NullVector as ArrowNullVector

class NullVector(
    // nullable = false used for vectors that only contain 'undefined' values
    override var name: String, override var nullable: Boolean = true, override var valueCount: Int = 0
) : MonoVector() {
    override val vectors = emptyList<Vector>()

    override val arrowType: ArrowType = NULL_TYPE
    override val monoType = Null

    // a non-nullable NULL vector holds only 'undefined' (no values seen) — the lattice bottom Nothing,
    // distinct from the nullable Null marker. Keeps a declared-but-empty column (CREATE TABLE foo (a))
    // from polluting to Maybe on its first insert: Nothing ⊔ I64 = I64, whereas Null ⊔ I64 = Maybe(I64).
    override val type get() = if (nullable) Null else Nothing

    override fun isNull(idx: Int) = true

    override fun writeUndefined() {
        valueCount++
    }

    override fun ensureCapacity(valueCount: Int) {
        this.valueCount = this.valueCount.coerceAtLeast(valueCount)
    }

    override fun setNull(idx: Int) {
        valueCount = valueCount.coerceAtLeast(idx + 1)
    }

    override fun writeNull() {
        if (!nullable) nullable = true
        valueCount++
    }

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = error("NullVector getObject0")

    override fun writeObject0(value: Any) = throw InvalidWriteObjectException(this, value)

    override fun writeValue0(v: ValueReader) = writeNull()

    override val metadataFlavours get() = emptyList<MetadataFlavour>()

    override fun hashCode0(idx: Int, hasher: Hasher) = error("hashCode0 called on NullVector")

    override fun sumInto(outVec: Vector): VectorSummer {
        check(outVec is MonoVector)

        return VectorSummer { _, groupIdx ->
            outVec.ensureCapacity(groupIdx + 1)
        }
    }

    override fun maybePromote(al: BufferAllocator, targetType: ArrowType, targetNullable: Boolean): Vector =
        if (targetType == arrowType) this
        else
            al.openVector(this.name, targetType, targetNullable)
                .also { newVec ->
                    repeat(this.valueCount) {
                        // if we've only ever written undefined, write undefineds in the new vec
                        if (nullable) newVec.writeNull() else newVec.writeUndefined()
                    }
                }

    override fun rowCopier(dest: VectorWriter) =
        if (dest is DenseUnionVector) dest.rowCopier0(this)
        else {
            RowCopier { dest.writeNull() }
        }

    override fun rowCopier0(src: VectorReader) = RowCopier { writeNull() }

    override fun appendRange0(src: VectorReader, startIdx: Int, len: Int) {
        if (!nullable) nullable = true
        valueCount += len
    }

    // as a source: write nulls into any dest (mirrors rowCopier above)
    override fun appendRangeTo(dest: VectorWriter, startIdx: Int, len: Int) {
        if (dest is DenseUnionVector) dest.appendRange0(this, startIdx, len)
        else repeat(len) { dest.writeNull() }
    }

    override fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), valueCount.toLong()))
    }

    override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirst()
        valueCount = node.length
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is ArrowNullVector)

        valueCount = vec.valueCount
    }

    override fun clear() {
        valueCount = 0
    }

    override fun close() {
    }

    override fun openSlice(al: BufferAllocator) = NullVector(name, nullable, valueCount)
}
