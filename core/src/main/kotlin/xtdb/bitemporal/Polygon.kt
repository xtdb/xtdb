package xtdb.bitemporal

import com.carrotsearch.hppc.LongArrayList
import xtdb.trie.EventRowPointer
import kotlin.math.min

@JvmRecord
data class Polygon(val validTimes: LongArrayList, val sysTimeCeilings: LongArrayList) : IPolygonReader {
    constructor() : this(LongArrayList(), LongArrayList())

    override val validTimeRangeCount
        get() = sysTimeCeilings.elementsCount

    override fun getValidFrom(rangeIdx: Int) = validTimes[rangeIdx]

    override fun getValidTo(rangeIdx: Int) = validTimes[rangeIdx + 1]

    override fun getSystemTo(rangeIdx: Int) = sysTimeCeilings[rangeIdx]

    fun calculateFor(ceiling: Ceiling, validFrom: Long, validTo: Long) {
        validTimes.clear()
        sysTimeCeilings.clear()

        var validTime = validFrom

        var ceilIdx = ceiling.getCeilingIndex(validFrom)

        do {
            var ceilValidTo: Long

            while (true) {
                ceilValidTo = ceiling.getValidTo(ceilIdx)
                if (ceilValidTo > validTime) break
                ceilIdx++
            }

            validTimes.add(validTime)
            sysTimeCeilings.add(ceiling.getSystemTime(ceilIdx))

            validTime = min(ceilValidTo, validTo)
        } while (validTime != validTo)

        validTimes.add(validTime)
    }

    fun calculateFor(ceiling: Ceiling, evPtr: EventRowPointer) {
        calculateFor(ceiling, evPtr.validFrom, evPtr.validTo)
    }
}
