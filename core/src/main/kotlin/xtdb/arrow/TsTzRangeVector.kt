package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.TimeUnit.MICROSECOND
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.time.microsAsInstant
import xtdb.types.ZonedDateTimeRange
import xtdb.vector.extensions.TsTzRangeType
import java.time.ZoneId
import java.time.ZonedDateTime

class TsTzRangeVector(override val inner: FixedSizeListVector) : ExtensionVector(), MetadataFlavour.Presence {
    companion object {
        private fun Long.toZdt(): ZonedDateTime = microsAsInstant.atZone(ZoneId.of("UTC"))
    }

    override val arrowType = TsTzRangeType

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) =
        inner.listElements.let { elVec ->
            ZonedDateTimeRange(
                elVec.getLong(idx * 2).takeUnless { it == Long.MIN_VALUE }?.toZdt(),
                elVec.getLong(idx * 2 + 1).takeUnless { it == Long.MAX_VALUE }?.toZdt()
            )
        }

    override fun writeObject0(value: Any) = when (value) {
        is ZonedDateTimeRange -> {
            inner.getListElements(FieldType.notNullable(Timestamp(MICROSECOND, "UTC"))).let { elVec ->
                if (value.from != null) elVec.writeObject(value.from) else elVec.writeLong(Long.MIN_VALUE)
                if (value.to != null) elVec.writeObject(value.to) else elVec.writeLong(Long.MAX_VALUE)
            }
            inner.endList()
        }

        is ListValueReader -> inner.writeObject(value)

        else -> throw InvalidWriteObjectException(fieldType, value)
    }

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) = TsTzRangeVector(inner.openSlice(al))
}