package xtdb.vector.extensions

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.types.IntervalUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.arrow.IntervalMonthDayMicroVector

object IntervalMDMType : XtExtensionType("xt/intervalDMMType", Interval(IntervalUnit.MONTH_DAY_NANO)) {
    init {
        ExtensionTypeRegistry.register(this)
    }

    override fun deserialize(serializedData: String): ArrowType = this

    override fun getNewVector(name: String, fieldType: FieldType, allocator: BufferAllocator): FieldVector =
        IntervalMonthDayMicroVector(name, allocator, fieldType)
}
