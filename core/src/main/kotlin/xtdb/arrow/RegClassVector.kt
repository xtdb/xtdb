package xtdb.arrow

import java.time.ZoneId
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.query.IKeyFn
import xtdb.arrow.VectorType.Scalar
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.hash.perRowHashes
import xtdb.types.RegClass
import xtdb.arrow.extensions.RegClassType
import xtdb.util.Hasher

class RegClassVector(override val inner: IntVector) : ExtensionVector(), MetadataFlavour.Presence {
    override val arrowType = RegClassType
    override val monoType = Scalar(arrowType)

    override fun getInt(idx: Int) = inner.getInt(idx)
    override fun writeInt(v: Int) = inner.writeInt(v)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = RegClass(inner.getInt(idx))

    override fun writeObject0(value: Any) = when (value) {
        is RegClass -> inner.writeInt(value.oid)
        else -> throw InvalidWriteObjectException(this, value)
    }

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) = RegClassVector(inner.openSlice(al))

    override fun hashTo(out: IntVector, hasher: Hasher, sel: IntArray?) = perRowHashes(this, out, sel, hasher)

    override fun equiRows(other: Vector, zone: ZoneId) = if (other is RegClassVector) equiObjects(this, other) else null
}
