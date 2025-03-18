package xtdb.vector.extensions

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.IntervalMonthDayNanoVector
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.types.IntervalMonthDayMicro

class IntervalMonthDayMicroVector(name: String, allocator: BufferAllocator, fieldType: FieldType) :
    XtExtensionVector<IntervalMonthDayNanoVector>(name, allocator, fieldType, IntervalMonthDayNanoVector(name, allocator)) {

    init {
        require(fieldType.type == IntervalMDMType)
    }

    override fun getObject0(index: Int): IntervalMonthDayMicro {
        val inner = underlyingVector.getObject(index)
        return IntervalMonthDayMicro(inner.period, inner.duration)
    }
}
