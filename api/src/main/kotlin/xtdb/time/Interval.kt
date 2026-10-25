package xtdb.time

import java.lang.Math.addExact
import java.lang.Math.multiplyExact
import java.lang.Math.negateExact
import java.lang.Math.subtractExact
import java.lang.Math.toIntExact
import java.time.Duration
import java.time.Period

data class Interval(val months: Int, val days: Int, val nanos: Long) {
    val period: Period get() = Period.of(0, months, days)
    val duration: Duration get() = Duration.ofNanos(nanos)

    override fun toString() = buildString {
        append("P")
        if (months / 12 != 0) append(months / 12).append('Y')
        if (months % 12 != 0) append(months % 12).append('M')
        if (days != 0) append(days).append('D')
        if (nanos != 0L) append(duration.toString().drop(1))
    }

    /**
     * @throws ArithmeticException if the period's total months don't fit an [Int], or the duration's nanos a [Long].
     */
    constructor(period: Period = Period.ZERO, duration: Duration = Duration.ZERO) : this(
        toIntExact(period.toTotalMonths()), period.days, duration.toNanos()
    )

    operator fun plus(other: Interval) =
        Interval(addExact(months, other.months), addExact(days, other.days), addExact(nanos, other.nanos))

    operator fun minus(other: Interval) =
        Interval(subtractExact(months, other.months), subtractExact(days, other.days), subtractExact(nanos, other.nanos))

    fun negated() = Interval(negateExact(months), negateExact(days), negateExact(nanos))

    fun multipliedBy(factor: Long) = Interval(
        toIntExact(multiplyExact(months.toLong(), factor)),
        toIntExact(multiplyExact(days.toLong(), factor)),
        multiplyExact(nanos, factor)
    )

    fun plusYears(years: Long) = Interval(addExact(months, toIntExact(multiplyExact(years, 12))), days, nanos)
    fun plusMonths(months: Long) = Interval(addExact(this.months, toIntExact(months)), days, nanos)
    fun plusWeeks(weeks: Long) = Interval(months, addExact(days, toIntExact(multiplyExact(weeks, 7))), nanos)
    fun plusDays(days: Long) = Interval(months, addExact(this.days, toIntExact(days)), nanos)
    fun plusHours(hours: Long) = Interval(months, days, addExact(nanos, multiplyExact(hours, 3600L * NANO_HZ)))
    fun plusMinutes(minutes: Long) = Interval(months, days, addExact(nanos, multiplyExact(minutes, 60L * NANO_HZ)))
    fun plusSeconds(seconds: Long) = Interval(months, days, addExact(nanos, multiplyExact(seconds, NANO_HZ)))

    companion object {
        @JvmField val ZERO = Interval(0, 0, 0L)
    }
}