package xtdb.arrow

import clojure.core.Vec
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.arrow.agg.VectorSummer
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher
import org.apache.arrow.vector.NullVector as ArrowNullVector

internal val NULL_TYPE = ArrowType.Null.INSTANCE

class NullVector(
    // nullable = false used for vectors that only contain 'undefined' values
    override var name: String, override var nullable: Boolean = true, override var valueCount: Int = 0
) : Vector() {
    override val vectors = emptyList<Vector>()

    override val arrowType: ArrowType = NULL_TYPE

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

    override fun writeObject0(value: Any) = throw InvalidWriteObjectException(fieldType, value)

    override fun writeValue0(v: ValueReader) = writeNull()

    override val metadataFlavours get() = emptyList<MetadataFlavour>()

    override fun hashCode0(idx: Int, hasher: Hasher) = error("hashCode0 called on NullVector")

    override fun sumInto(outVec: Vector) = VectorSummer { _, groupIdx ->
        outVec.ensureCapacity(groupIdx + 1)
    }

    override fun maybePromote(al: BufferAllocator, targetType: ArrowType, targetNullable: Boolean): Vector =
        if (targetType == arrowType) this
        else
            Field(this.name, FieldType(targetNullable, targetType, null), emptyList()).openVector(al)
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
