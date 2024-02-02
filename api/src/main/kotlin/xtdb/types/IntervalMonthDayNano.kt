package xtdb.types

import java.time.Duration
import java.time.Period

data class IntervalMonthDayNano(
    @JvmField val period: Period,
    @JvmField val duration: Duration
) {
    override fun toString(): String {
        return period.toString() + duration.toString().substring(1)
    }
}
