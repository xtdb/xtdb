package xtdb.vector.extensions

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.TimeStampMicroTZVector
import org.apache.arrow.vector.complex.FixedSizeListVector
import org.apache.arrow.vector.types.TimeUnit.MICROSECOND
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.time.InstantUtil.asMicros
import xtdb.time.microsAsInstant
import xtdb.types.ZonedDateTimeRange
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.apply

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
        private val elFieldType = FieldType.notNullable(Timestamp(MICROSECOND, "UTC"))
        private val elField = Field("\$data\$", elFieldType, null)

        val tsTzRangeField = Field("tsTzRange", FieldType.nullable(TsTzRangeType), listOf(elField))
    }

    private fun Long.toZdt(): ZonedDateTime = microsAsInstant.atZone(ZoneId.of("UTC"))

    override fun getObject0(index: Int): ZonedDateTimeRange =
        (underlyingVector.dataVector as TimeStampMicroTZVector).let { elVec ->
            ZonedDateTimeRange(
                elVec.get(index * 2).takeUnless { it == Long.MIN_VALUE }?.toZdt(),
                elVec.get(index * 2 + 1).takeUnless { it == Long.MAX_VALUE }?.toZdt()
            )
        }

    fun setObject(idx: Int, v: ZonedDateTimeRange) {
        underlyingVector.startNewValue(idx)

        underlyingVector.addOrGetVector<TimeStampMicroTZVector>(elFieldType).vector.apply {
            val from = v.from
            setSafe(idx * 2, if (from == null) Long.MIN_VALUE else from.toInstant().asMicros)

            val to = v.to
            setSafe(idx * 2 + 1, if (to == null) Long.MAX_VALUE else to.toInstant().asMicros)
        }
    }
}
