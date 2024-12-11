package xtdb.arrow

import clojure.lang.IFn
import com.cognitect.transit.Reader
import xtdb.api.query.IKeyFn
import xtdb.types.ClojureForm
import xtdb.util.requiringResolve
import xtdb.vector.extensions.TransitType
import java.io.ByteArrayInputStream

private val TRANSIT_MSGPACK_READER: IFn = requiringResolve("xtdb.serde/transit-msgpack-reader")

class TransitVector(override val inner: VarBinaryVector) : ExtensionVector(TransitType) {

    private fun transitReader(v: ByteArray): Reader = TRANSIT_MSGPACK_READER.invoke(ByteArrayInputStream(v)) as Reader

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = transitReader(inner.getObject0(idx, keyFn)).read<Any>()

    override fun writeObject0(value: Any) =
        when (value) {
            is ClojureForm, is xtdb.RuntimeException, is xtdb.IllegalArgumentException,
            -> inner.writeObject(requiringResolve("xtdb.serde/write-transit")(value) as ByteArray)

            else -> throw InvalidWriteObjectException(fieldType, value)
        }

}
