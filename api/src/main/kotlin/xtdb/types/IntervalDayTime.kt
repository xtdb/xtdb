package xtdb.types

import clojure.lang.Keyword
import xtdb.IllegalArgumentException
import java.time.Duration
import java.time.Period

data class IntervalDayTime(
    @JvmField val period: Period,
    @JvmField val duration: Duration
) {
    init {
        if (period.years != 0 || period.months != 0)
            throw IllegalArgumentException(Keyword.intern("xtdb/invalid-interval"), "Period can not contain years or months!")
    }

    override fun toString(): String {
        return period.toString() + duration.toString().substring(1)
    }
}
