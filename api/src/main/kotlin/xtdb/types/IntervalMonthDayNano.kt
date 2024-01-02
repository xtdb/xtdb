package xtdb.types

import java.time.Duration
import java.time.Period
import java.util.*

data class IntervalMonthDayNano(
    @get:JvmName("period") val period: Period,
    @get:JvmName("duration") val duration: Duration
) {
    override fun toString(): String {
        return period.toString() + duration.toString().substring(1)
    }
}
