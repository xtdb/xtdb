package xtdb.types

import clojure.lang.PersistentHashMap
import xtdb.IllegalArgumentException
import java.time.Duration
import java.time.Period
import java.util.*

data class IntervalDayTime(
    @get:JvmName("period") val period: Period,
    @get:JvmName("duration") val duration: Duration
) {
    init {
        if (period.years != 0 || period.months != 0)
            throw IllegalArgumentException("Period can not contain years or months!", PersistentHashMap.EMPTY, null)
    }

    override fun toString(): String {
        return period.toString() + duration.toString().substring(1)
    }
}
