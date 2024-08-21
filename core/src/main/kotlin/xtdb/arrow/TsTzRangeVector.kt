package xtdb.arrow

import org.apache.arrow.vector.types.TimeUnit.MICROSECOND
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.types.ZonedDateTimeRange
import xtdb.vector.extensions.TsTzRangeType
import java.time.ZonedDateTime

class TsTzRangeVector(override val inner: FixedSizeListVector) : ExtensionVector() {
    override val field
        get() = Field(name, FieldType(nullable, TsTzRangeType, null), inner.field.children)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) =
        inner.elementReader().let {
            ZonedDateTimeRange(
                it.getObject(idx * 2) as ZonedDateTime,
                it.getObject(idx * 2 + 1) as? ZonedDateTime
            )
        }

    override fun writeObject0(value: Any) = when (value) {
        is ZonedDateTimeRange -> {
            inner.elementWriter(FieldType.notNullable(Timestamp(MICROSECOND, "UTC"))).let {
                it.writeObject(value.from)
                if (value.to != null) it.writeObject(value.to) else it.writeLong(Long.MAX_VALUE)
            }
        }

        else -> TODO("promotion: ${value::class.simpleName}")
    }
}