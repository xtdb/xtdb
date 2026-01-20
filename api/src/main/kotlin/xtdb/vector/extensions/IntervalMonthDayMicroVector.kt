package xtdb.vector.extensions

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.IntervalMonthDayNanoVector
import org.apache.arrow.vector.holders.NullableIntervalMonthDayNanoHolder
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.time.Interval

class IntervalMonthDayMicroVector(name: String, allocator: BufferAllocator, fieldType: FieldType) :
    XtExtensionVector<IntervalMonthDayNanoVector>(name, allocator, fieldType, IntervalMonthDayNanoVector(name, allocator)) {

    init {
        require(fieldType.type == IntervalMDMType)
    }

    override fun getObject0(index: Int): Interval {
        val holder = NullableIntervalMonthDayNanoHolder()
        underlyingVector.get(index, holder)
        return Interval(holder.months, holder.days, holder.nanoseconds)
    }
}
