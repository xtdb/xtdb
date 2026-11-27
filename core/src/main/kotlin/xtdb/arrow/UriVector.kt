package xtdb.arrow

import java.time.ZoneId
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.query.IKeyFn
import xtdb.arrow.VectorType.Scalar
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.arrow.extensions.UriType
import xtdb.hash.perRowHashes
import xtdb.util.Hasher
import java.net.URI

class UriVector(override val inner: Utf8Vector) : ExtensionVector(), MetadataFlavour.Bytes {

    override val arrowType = UriType
    override val monoType = Scalar(arrowType)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) =
        URI.create(inner.getObject0(idx, keyFn))

    override fun writeObject0(value: Any) = when (value) {
        is URI -> inner.writeObject(value.toString())
        else -> throw InvalidWriteObjectException(this, value)
    }

    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) = UriVector(inner.openSlice(al))

    override fun hashTo(out: IntVector, hasher: Hasher, sel: IntArray?) = perRowHashes(this, out, sel, hasher)

    override fun equiRows(other: Vector, zone: ZoneId) = if (other is UriVector) equiObjects(this, other) else null
    override fun compareRows(other: Vector, zone: ZoneId) = if (other is UriVector) compareBytes(this, other) else null
}