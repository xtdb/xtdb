package xtdb.arrow

import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.vector.extensions.UuidType
import java.nio.ByteBuffer
import java.util.*

class UuidVector(override val inner: FixedSizeBinaryVector) : ExtensionVector() {

    override val arrowField: Field get() = Field(name, FieldType(nullable, UuidType, null), emptyList())

    override fun getObject0(idx: Int) =
        ByteBuffer.wrap(inner.getBytes(idx)).let { buf ->
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