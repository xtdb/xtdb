package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher
import xtdb.vector.extensions.SetType

class SetVector(override val inner: ListVector) : ExtensionVector(), MetadataFlavour.Set {

    override val type = SetType

    override val listElements get() = inner.listElements
    override fun getListElements(fieldType: FieldType) = inner.getListElements(fieldType)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = inner.getObject0(idx, keyFn).toSet()

    override fun writeObject0(value: Any) =
        if (value !is Set<*>) throw InvalidWriteObjectException(fieldType, value)
        else inner.writeObject(value.toList())

    override fun hashCode0(idx: Int, hasher: Hasher): Int {
        val elVector = inner.listElements
        return (inner.getListStartIndex(idx) until inner.getListEndIndex(idx))
            .sumOf { elIdx -> elVector.hashCode(elIdx, hasher) }
    }

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) = SetVector(inner.openSlice(al))
}
