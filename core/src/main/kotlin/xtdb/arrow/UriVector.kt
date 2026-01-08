package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.vector.extensions.UriType
import java.net.URI

class UriVector(override val inner: Utf8Vector) : ExtensionVector(), MetadataFlavour.Bytes {

    override val arrowType = UriType

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) =
        URI.create(inner.getObject0(idx, keyFn))

    override fun writeObject0(value: Any) = when (value) {
        is URI -> inner.writeObject(value.toString())
        else -> throw InvalidWriteObjectException(this, value)
    }

    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())

    override val metadataFlavours get() = listOf(this)

    override fun valueReader() = object : ValueReader {
        override var pos = 0
        override val isNull: Boolean get() = this@UriVector.isNull(pos)
        override fun readObject(): Any? = getObject(pos)
    }

    override fun openSlice(al: BufferAllocator) = UriVector(inner.openSlice(al))
}