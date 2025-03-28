package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.TimeUnit.MICROSECOND
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.types.ZonedDateTimeRange
import xtdb.vector.extensions.TsTzRangeType
import java.time.ZonedDateTime

class TsTzRangeVector(override val inner: FixedSizeListVector) : ExtensionVector(), MetadataFlavour.Presence {
    override val type = TsTzRangeType

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) =
        inner.listElements.let {
            ZonedDateTimeRange(
                it.getObject(idx * 2) as ZonedDateTime,
                it.getObject(idx * 2 + 1) as? ZonedDateTime
            )
        }

    override fun writeObject0(value: Any) = when (value) {
        is ZonedDateTimeRange -> {
            inner.getListElements(FieldType.notNullable(Timestamp(MICROSECOND, "UTC"))).let {
                it.writeObject(value.from)
                if (value.to != null) it.writeObject(value.to) else it.writeLong(Long.MAX_VALUE)
            }
            inner.endList()
        }

        else -> throw InvalidWriteObjectException(fieldType, value)
    }

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) = TsTzRangeVector(inner.openSlice(al))
}