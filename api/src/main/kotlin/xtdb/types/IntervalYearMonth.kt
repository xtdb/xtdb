package xtdb.types

import java.time.Period
import java.util.*

data class IntervalYearMonth(@JvmField val period: Period) {
    override fun toString(): String {
        return period.toString()
    }
}
