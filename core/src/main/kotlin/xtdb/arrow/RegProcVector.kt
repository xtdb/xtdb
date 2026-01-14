package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.query.IKeyFn
import xtdb.arrow.VectorType.Scalar
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.types.RegProc
import xtdb.vector.extensions.RegProcType

class RegProcVector(override val inner: IntVector) : ExtensionVector(), MetadataFlavour.Presence {
    override val arrowType = RegProcType
    override val monoType = Scalar(arrowType)

    override fun getInt(idx: Int) = inner.getInt(idx)
    override fun writeInt(v: Int) = inner.writeInt(v)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = RegProc(inner.getInt(idx))

    override fun writeObject0(value: Any) = when (value) {
        is RegProc -> inner.writeInt(value.oid)
        else -> throw InvalidWriteObjectException(this, value)
    }

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) = RegProcVector(inner.openSlice(al))
}
