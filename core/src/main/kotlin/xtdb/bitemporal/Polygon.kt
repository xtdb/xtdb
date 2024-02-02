package xtdb.bitemporal

import com.carrotsearch.hppc.LongArrayList
import kotlin.math.min

@JvmRecord
data class Polygon(val validTimes: LongArrayList, val sysTimeCeilings: LongArrayList) : IPolygonReader {
    constructor() : this(LongArrayList(), LongArrayList())

    override val validTimeRangeCount
        get() = sysTimeCeilings.elementsCount

    override fun getValidFrom(rangeIdx: Int) = validTimes[rangeIdx]

    override fun getValidTo(rangeIdx: Int) = validTimes[rangeIdx + 1]

    override fun getSystemTo(rangeIdx: Int) = sysTimeCeilings[rangeIdx]

    fun calculateFor(ceiling: Ceiling, inPolygon: IPolygonReader) {
        validTimes.clear()
        sysTimeCeilings.clear()

        var ceilIdx = 0

        val validTimeRangeCount = inPolygon.validTimeRangeCount
        assert(validTimeRangeCount > 0)
        var rangeIdx = 0

        var validTime = inPolygon.getValidFrom(rangeIdx)
        var validTo = inPolygon.getValidTo(rangeIdx)
        var systemTo = inPolygon.getSystemTo(rangeIdx)

        while (true) {
            if (systemTo != Long.MAX_VALUE) {
                validTimes.add(validTime)
                sysTimeCeilings.add(systemTo)
                validTime = validTo
            } else {
                var ceilValidTo: Long

                while (true) {
                    ceilValidTo = ceiling.getValidTo(ceilIdx)
                    if (ceilValidTo > validTime) break
                    ceilIdx++
                }

                validTimes.add(validTime)
                sysTimeCeilings.add(ceiling.getSystemTime(ceilIdx))

                validTime = min(ceilValidTo.toDouble(), validTo.toDouble()).toLong()
            }

            if (validTime == validTo) {
                if (++rangeIdx == validTimeRangeCount) break

                validTo = inPolygon.getValidTo(rangeIdx)
                systemTo = inPolygon.getSystemTo(rangeIdx)
            }
        }

        validTimes.add(validTime)
    }
}
