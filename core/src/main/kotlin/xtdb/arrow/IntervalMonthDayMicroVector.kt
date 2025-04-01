package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.types.IntervalMonthDayMicro
import xtdb.types.IntervalMonthDayNano
import xtdb.util.Hasher
import xtdb.vector.extensions.IntervalMDMType

class IntervalMonthDayMicroVector(
    override val inner: IntervalMonthDayNanoVector
) : ExtensionVector(), MetadataFlavour.Presence {

    override val type = IntervalMDMType

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): IntervalMonthDayMicro {
        val innerMDN = inner.getObject0(idx, keyFn)
        return IntervalMonthDayMicro(innerMDN.period, innerMDN.duration)
    }

    override fun writeObject0(value: Any) {
        if (value !is IntervalMonthDayMicro) throw InvalidWriteObjectException(fieldType, value)
        else inner.writeObject(IntervalMonthDayNano(value.period, value.duration))
    }

    override val metadataFlavours get() = listOf(this)

    override fun hashCode0(idx: Int, hasher: Hasher) = inner.hashCode0(idx, hasher)

    override fun openSlice(al: BufferAllocator) = IntervalMonthDayMicroVector(inner.openSlice(al))
}