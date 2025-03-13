package xtdb.vector.extensions

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.FixedSizeListVector
import org.apache.arrow.vector.types.TimeUnit.MICROSECOND
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.types.ZonedDateTimeRange
import xtdb.vector.from
import java.time.Instant.EPOCH
import java.time.ZoneOffset.UTC
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit.MICROS

class TsTzRangeVector(name: String, allocator: BufferAllocator, fieldType: FieldType) :
    XtExtensionVector<FixedSizeListVector>(
        Field(name, fieldType, listOf(elField)),
        allocator,
        FixedSizeListVector(
            Field(name, FieldType.notNullable(FixedSizeList(2)), listOf(elField)),
            allocator, null
        )
    ) {

    init {
        require(fieldType.type is TsTzRangeType)
    }

    companion object {
        private val elField =
            Field("\$data\$", FieldType.notNullable(Timestamp(MICROSECOND, "UTC")), null)

        val tsTzRangeField = Field("tsTzRange", FieldType.nullable(TsTzRangeType), listOf(elField))
    }

    private fun Long.toZdt(): ZonedDateTime = EPOCH.plus(this, MICROS).atZone(UTC)

    override fun getObject0(index: Int): ZonedDateTimeRange =
        from(underlyingVector.dataVector).let { elVec ->
            ZonedDateTimeRange(
                elVec.getLong(index * 2).toZdt(),
                elVec.getLong(index * 2 + 1).takeUnless { it == Long.MAX_VALUE }?.toZdt()
            )
        }
}
