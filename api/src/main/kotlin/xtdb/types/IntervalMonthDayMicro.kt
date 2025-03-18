package xtdb.types

import java.time.Duration
import java.time.Period

data class IntervalMonthDayMicro(
    @JvmField val period: Period,
    @JvmField val duration: Duration
) {
    init {
        require(duration.nano % 1000 == 0) { "Month Day Micro Interval only supports up to microsecond precision (6)" }
    }

    override fun toString(): String {
        return period.toString() + duration.toString().substring(1)
    }
}
