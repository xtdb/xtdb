package xtdb.arrow

import java.time.ZoneId
import clojure.lang.IFn
import com.cognitect.transit.Reader
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.query.IKeyFn
import xtdb.arrow.VectorType.Scalar
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.hash.perRowHashes
import xtdb.util.Hasher
import xtdb.util.requiringResolve
import xtdb.arrow.extensions.TransitType
import java.io.ByteArrayInputStream

private val TRANSIT_MSGPACK_READER: IFn = requiringResolve("xtdb.serde/transit-msgpack-reader")

class TransitVector(override val inner: VarBinaryVector) : ExtensionVector(), MetadataFlavour.Presence {

    override val arrowType = TransitType
    override val monoType = Scalar(arrowType)

    private fun transitReader(v: ByteArray): Reader = TRANSIT_MSGPACK_READER.invoke(ByteArrayInputStream(v)) as Reader

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = transitReader(inner.getObject0(idx, keyFn)).read<Any>()

    override fun writeObject0(value: Any) =
        inner.writeBytes(requiringResolve("xtdb.serde/write-transit")(value) as ByteArray)

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) = TransitVector(inner.openSlice(al))

    override fun hashTo(out: IntVector, hasher: Hasher, sel: IntArray?) = perRowHashes(this, out, sel, hasher)

    override fun equiRows(other: Vector, zone: ZoneId) = if (other is TransitVector) equiObjects(this, other) else null
}
