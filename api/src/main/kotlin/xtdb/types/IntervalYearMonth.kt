package xtdb.types

import java.time.Period
import java.util.*

data class IntervalYearMonth(@get:JvmName("period") val period: Period) {
    override fun toString(): String {
        return period.toString()
    }
}
