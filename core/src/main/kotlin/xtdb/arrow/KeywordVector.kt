package xtdb.arrow

import clojure.lang.Keyword
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.query.IKeyFn
import xtdb.arrow.VectorType.Scalar
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.vector.extensions.KeywordType

class KeywordVector(override val inner: Utf8Vector): ExtensionVector(), MetadataFlavour.Bytes {

    override val arrowType = KeywordType
    override val monoType = Scalar(arrowType)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = Keyword.intern(inner.getObject0(idx, keyFn))

    override fun writeObject0(value: Any) = when(value) {
        is Keyword -> inner.writeObject(value.sym.toString())
        else -> throw InvalidWriteObjectException(this, value)
    }

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) = KeywordVector(inner.openSlice(al))
}