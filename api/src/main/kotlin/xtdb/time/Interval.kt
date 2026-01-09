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

    fun plusYears(years: Long) = Interval(months + (years * 12).toInt(), days, nanos)
    fun plusMonths(months: Long) = Interval(this.months + months.toInt(), days, nanos)
    fun plusWeeks(weeks: Long) = Interval(months, days + (weeks * 7).toInt(), nanos)
    fun plusDays(days: Long) = Interval(months, this.days + days.toInt(), nanos)
    fun plusHours(hours: Long) = Interval(months, days, nanos + hours * 3600 * NANO_HZ)
    fun plusMinutes(minutes: Long) = Interval(months, days, nanos + minutes * 60 * NANO_HZ)
    fun plusSeconds(seconds: Long) = Interval(months, days, nanos + seconds * NANO_HZ)

    companion object {
        @JvmField val ZERO = Interval(0, 0, 0L)
    }
}