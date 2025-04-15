package xtdb.time

import java.time.Duration
import java.time.Period

data class Interval(val months: Int, val days: Int, val nanos: Long) {
    val period: Period get() = Period.of(0, months, days)
    val duration: Duration get() = Duration.ofNanos(nanos)

    override fun toString() = buildString {
        append("P")
        if (months != 0 || days != 0) append(period.toString().drop(1))
        if (nanos != 0L) append(duration.toString().drop(1))
    }

    constructor(period: Period = Period.ZERO, duration: Duration = Duration.ZERO) : this(
        period.toTotalMonths().toInt(), period.days, duration.toNanos()
    )
}