package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.types.RegClass
import xtdb.vector.extensions.RegClassType

class RegClassVector(override val inner: IntVector) : ExtensionVector(), MetadataFlavour.Presence {
    override val type = RegClassType

    override fun getInt(idx: Int) = inner.getInt(idx)
    override fun writeInt(v: Int) = inner.writeInt(v)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = RegClass(inner.getInt(idx))

    override fun writeObject0(value: Any) = when (value) {
        is RegClass -> inner.writeInt(value.oid)
        else -> throw InvalidWriteObjectException(fieldType, value)
    }

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) = RegClassVector(inner.openSlice(al))
}
