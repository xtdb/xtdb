package xtdb.vector.extensions

import clojure.lang.IFn
import com.cognitect.transit.Reader
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VarBinaryVector
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.util.requiringResolve
import java.io.ByteArrayInputStream

private val TRANSIT_MSGPACK_READER: IFn = requiringResolve("xtdb.serde/transit-msgpack-reader")

class TransitVector(name: String, allocator: BufferAllocator, fieldType: FieldType) :
    XtExtensionVector<VarBinaryVector>(name, allocator, fieldType, VarBinaryVector(name, allocator)) {

    init {
        require(fieldType.type == TransitType)
    }

    private fun transitReader(v: ByteArray): Reader = TRANSIT_MSGPACK_READER.invoke(ByteArrayInputStream(v)) as Reader

    override fun getObject0(index: Int): Any = transitReader(underlyingVector[index]).read()
}
