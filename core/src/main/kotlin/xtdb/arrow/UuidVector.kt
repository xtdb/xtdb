package xtdb.arrow

import xtdb.api.query.IKeyFn
import xtdb.vector.extensions.UuidType
import java.nio.ByteBuffer
import java.util.*

class UuidVector(override val inner: FixedSizeBinaryVector) : ExtensionVector(UuidType) {

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) =
        ByteBuffer.wrap(inner.getObject0(idx, keyFn)).let { buf ->
            UUID(buf.getLong(), buf.getLong())
        }

    override fun writeObject0(value: Any) = when (value) {
        is UUID -> ByteBuffer.allocate(16).run {
            putLong(value.mostSignificantBits)
            putLong(value.leastSignificantBits)
            inner.writeObject(array())
        }

        else -> TODO("promotion: ${value::class.simpleName}")
    }
}