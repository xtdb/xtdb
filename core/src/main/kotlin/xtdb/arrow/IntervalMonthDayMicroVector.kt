package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.types.Interval
import xtdb.util.Hasher
import xtdb.vector.extensions.IntervalMDMType

class IntervalMonthDayMicroVector(
    override val inner: IntervalMonthDayNanoVector
) : ExtensionVector(), MetadataFlavour.Presence {

    override val type = IntervalMDMType

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): Interval.MonthDayMicro =
        inner.getObject0(idx, keyFn).let { Interval.MonthDayMicro(it.months, it.days, it.nanos) }

    override fun writeObject0(value: Any) {
        if (value !is Interval.MonthDayMicro) throw InvalidWriteObjectException(fieldType, value)
        else inner.writeObject(Interval.MonthDayNano(value.months, value.days, value.nanos))
    }

    override val metadataFlavours get() = listOf(this)

    override fun hashCode0(idx: Int, hasher: Hasher) = inner.hashCode0(idx, hasher)

    override fun openSlice(al: BufferAllocator) = IntervalMonthDayMicroVector(inner.openSlice(al))
}