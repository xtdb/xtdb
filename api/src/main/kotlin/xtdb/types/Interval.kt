package xtdb.types

import java.time.Duration
import java.time.Period

sealed interface Interval {
    val period: Period
    val duration: Duration

    companion object {
        @JvmStatic
        fun ofMonths(months: Int) = Month(months)

        @JvmStatic
        fun ofMonths(period: Period): Month {
            require(period.days == 0) { "invalid Month interval: $period" }
            return ofMonths(period.toTotalMonths().toInt())
        }

        @JvmStatic
        fun ofDayTime(days: Int, millis: Int) = DayTime(days, millis)

        @JvmStatic
        fun ofDayTime(p: Period, d: Duration): DayTime {
            require(p.years == 0 && p.months == 0) { "invalid DayTime period: $p" }
            return ofDayTime(p.days, d.toMillis().toInt())
        }

        @JvmStatic
        fun ofMicros(months: Int, days: Int, nanos: Long) = MonthDayMicro(months, days, nanos)

        @JvmStatic
        fun ofMicros(period: Period = Period.ZERO, duration: Duration = Duration.ZERO) =
            ofMicros(period.toTotalMonths().toInt(), period.days, duration.toNanos())

        @JvmStatic
        fun ofNanos(months: Int, days: Int, nanos: Long) = MonthDayNano(months, days, nanos)

        @JvmStatic
        fun ofNanos(period: Period = Period.ZERO, duration: Duration = Duration.ZERO) =
            ofNanos(period.toTotalMonths().toInt(), period.days, duration.toNanos())
    }

    data class Month(val months: Int) : Interval {
        override val period: Period get() = Period.ofMonths(months)
        override val duration: Duration get() = Duration.ZERO

        override fun toString() = period.toString()
    }

    data class DayTime(val days: Int, val millis: Int) : Interval {
        override val period: Period get() = Period.ofDays(days)
        override val duration: Duration get() = Duration.ofMillis(millis.toLong())

        override fun toString() = "${period}${duration.toString().drop(1)}"
    }

    data class MonthDayMicro(val months: Int, val days: Int, val nanos: Long) : Interval {
        init {
            require(nanos % 1000L == 0L) { "Month Day Micro Interval only supports up to microsecond precision (6)" }
        }

        override val period: Period get() = Period.of(0, months, days)
        override val duration: Duration get() = Duration.ofNanos(nanos)

        override fun toString() = "${period}${duration.toString().drop(1)}"
    }

    data class MonthDayNano(val months: Int, val days: Int, val nanos: Long) : Interval {
        override val period: Period get() = Period.of(0, months, days)
        override val duration: Duration get() = Duration.ofNanos(nanos)

        override fun toString() = "${period}${duration.toString().drop(1)}"
    }
}