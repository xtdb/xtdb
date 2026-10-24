package xtdb.time

import org.apache.arrow.vector.PeriodDuration
import xtdb.InternalApi
import java.lang.Math.addExact
import java.lang.Math.floorDiv
import java.lang.Math.floorMod
import java.lang.Math.multiplyExact
import java.lang.Math.subtractExact
import java.time.Duration
import java.time.LocalDate
import java.time.Period
import java.time.temporal.IsoFields

/**
 * Zone and unit arithmetic over the expression engine's epoch-unit `long`s.
 *
 * - `hz` is a timestamp's units per second: 1, 1_000, 1_000_000 or 1_000_000_000.
 * - A local timestamp is its wall-clock reading counted from the epoch as though it were UTC - the representation `timestamp-local` already has.
 * - Days are epoch days, as `LocalDate.toEpochDay`.
 *
 * @suppress
 */
// `@InternalApi` rather than `internal`: the expression engine's generated Clojure calls it over interop,
// which can't resolve a mangled `internal` name - it can only go `internal` if codegen stops emitting these calls.
@InternalApi
object TemporalMath {
    private const val SECS_PER_DAY = 86_400L
    private const val NANOS_PER_SEC = 1_000_000_000L

    @JvmStatic
    fun toLocal(ts: Long, hz: Long, zone: ZoneOffsets): Long =
        addExact(ts, multiplyExact(zone.offsetAt(floorDiv(ts, hz)).toLong(), hz))

    /** resolves [localTs] in [zone] preferring [originalTs]'s offset, as `ZonedDateTime.with(LocalDateTime)` does */
    @JvmStatic
    fun fromLocalNear(localTs: Long, hz: Long, zone: ZoneOffsets, originalTs: Long): Long {
        val offset = zone.offsetForLocal(floorDiv(localTs, hz), zone.offsetAt(floorDiv(originalTs, hz)))
        return subtractExact(localTs, multiplyExact(offset.toLong(), hz))
    }

    /** resolves [localTs] in [zone] as `LocalDateTime.atZone` does */
    @JvmStatic
    fun fromLocal(localTs: Long, hz: Long, zone: ZoneOffsets): Long {
        val offset = zone.offsetForLocal(floorDiv(localTs, hz), ZoneOffsets.NO_PREFERRED_OFFSET)
        return subtractExact(localTs, multiplyExact(offset.toLong(), hz))
    }

    @JvmStatic
    fun startOfDay(epochDay: Long, hz: Long, zone: ZoneOffsets): Long = multiplyExact(zone.startOfDay(epochDay), hz)

    @JvmStatic
    fun epochDay(localTs: Long, hz: Long): Long = floorDiv(localTs, hz * SECS_PER_DAY)

    /** units since midnight, in the timestamp's own unit */
    @JvmStatic
    fun unitOfDay(localTs: Long, hz: Long): Long = floorMod(localTs, hz * SECS_PER_DAY)

    @JvmStatic
    fun dayToLocal(epochDay: Long, hz: Long): Long = multiplyExact(epochDay, hz * SECS_PER_DAY)

    @JvmStatic
    fun secondOfDay(localTs: Long, hz: Long): Int = (floorMod(localTs, hz * SECS_PER_DAY) / hz).toInt()

    @JvmStatic
    fun year(epochDay: Long): Int = LocalDate.ofEpochDay(epochDay).year

    @JvmStatic
    fun month(epochDay: Long): Int = LocalDate.ofEpochDay(epochDay).monthValue

    @JvmStatic
    fun dayOfMonth(epochDay: Long): Int = LocalDate.ofEpochDay(epochDay).dayOfMonth

    @JvmStatic
    fun dayOfYear(epochDay: Long): Int = LocalDate.ofEpochDay(epochDay).dayOfYear

    @JvmStatic
    fun quarter(epochDay: Long): Int = LocalDate.ofEpochDay(epochDay).get(IsoFields.QUARTER_OF_YEAR)

    /** Monday = 1 .. Sunday = 7 */
    @JvmStatic
    fun isoDayOfWeek(epochDay: Long): Int = LocalDate.ofEpochDay(epochDay).dayOfWeek.value

    /** Sunday = 0 .. Saturday = 6 */
    @JvmStatic
    fun dayOfWeek(epochDay: Long): Int = isoDayOfWeek(epochDay) % 7

    @JvmStatic
    fun isoWeek(epochDay: Long): Int = LocalDate.ofEpochDay(epochDay).get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)

    /**
     * `AGE(end, start)` over two local timestamps: the calendar period between their dates,
     * one day short where the time of day hasn't yet come round, and the sub-day remainder as the duration.
     */
    @JvmStatic
    fun ageInterval(endTs: Long, endHz: Long, startTs: Long, startHz: Long): PeriodDuration {
        val endDay = epochDay(endTs, endHz)
        val startDay = epochDay(startTs, startHz)
        val endNanoOfDay = unitOfDay(endTs, endHz) * (NANOS_PER_SEC / endHz)
        val startNanoOfDay = unitOfDay(startTs, startHz) * (NANOS_PER_SEC / startHz)

        val period = Period.between(LocalDate.ofEpochDay(startDay), LocalDate.ofEpochDay(endDay))
        val dayAdjustment = when {
            period.isNegative && startNanoOfDay < endNanoOfDay -> 1
            !period.isNegative && period.days != 0 && startNanoOfDay > endNanoOfDay -> -1
            else -> 0
        }

        val between = Duration.ofSeconds(
            addExact(multiplyExact(endDay - startDay, SECS_PER_DAY), endNanoOfDay / NANOS_PER_SEC - startNanoOfDay / NANOS_PER_SEC),
            endNanoOfDay % NANOS_PER_SEC - startNanoOfDay % NANOS_PER_SEC
        )

        return PeriodDuration(period.plusDays(dayAdjustment.toLong()), between.minusDays(between.toDays()))
    }

    private fun plusPeriod(epochDay: Long, period: Period, sign: Long): Long {
        val date = LocalDate.ofEpochDay(epochDay)
        return (if (sign > 0) date.plus(period) else date.minus(period)).toEpochDay()
    }

    // `LocalDateTime.minus(duration)` floors the result into the timestamp's unit, so subtracting takes the duration's ceiling
    private fun durationUnits(duration: Duration, hz: Long, sign: Long): Long {
        val secs = multiplyExact(duration.seconds, hz)
        val nanosPerUnit = NANOS_PER_SEC / hz
        return if (sign > 0) addExact(secs, duration.nano / nanosPerUnit)
        else subtractExact(Math.negateExact(secs), (duration.nano + nanosPerUnit - 1) / nanosPerUnit)
    }

    private fun localPlusPeriod(localTs: Long, hz: Long, period: Period, sign: Long): Long =
        if (period.isZero) localTs
        else addExact(dayToLocal(plusPeriod(epochDay(localTs, hz), period, sign), hz), unitOfDay(localTs, hz))

    /** `LocalDate.plus(interval.period)`, or `minus` where [sign] is -1 */
    @JvmStatic
    fun datePlusInterval(epochDay: Long, interval: PeriodDuration, sign: Long): Long =
        plusPeriod(epochDay, interval.period, sign)

    /** `LocalDateTime.plus(interval.period).plus(interval.duration)`, or `minus` where [sign] is -1 */
    @JvmStatic
    fun localPlusInterval(localTs: Long, hz: Long, interval: PeriodDuration, sign: Long): Long =
        addExact(localPlusPeriod(localTs, hz, interval.period, sign), durationUnits(interval.duration, hz, sign))

    /**
     * `ZonedDateTime.plus(interval.period).plus(interval.duration)`, or `minus` where [sign] is -1:
     * the period moves the wall-clock reading, which then resolves near [ts]'s offset; the duration moves the instant.
     */
    @JvmStatic
    fun tzPlusInterval(ts: Long, hz: Long, zone: ZoneOffsets, interval: PeriodDuration, sign: Long): Long {
        val period = interval.period
        val moved =
            if (period.isZero) ts
            else {
                val second = floorDiv(ts, hz)
                val movedSecond = fromLocalNear(localPlusPeriod(toLocal(second, 1, zone), 1, period, sign), 1, zone, second)
                addExact(multiplyExact(movedSecond, hz), floorMod(ts, hz))
            }
        return addExact(moved, durationUnits(interval.duration, hz, sign))
    }

    /** the first day of [epochDay]'s year, rounded towards zero to a multiple of [yearMultiple] */
    @JvmStatic
    fun truncYear(epochDay: Long, yearMultiple: Long): Long {
        val year = LocalDate.ofEpochDay(epochDay).year
        return LocalDate.of(Math.toIntExact((year / yearMultiple) * yearMultiple), 1, 1).toEpochDay()
    }

    @JvmStatic
    fun truncQuarter(epochDay: Long): Long {
        val date = LocalDate.ofEpochDay(epochDay)
        return date.withMonth(date.month.firstMonthOfQuarter().value).withDayOfMonth(1).toEpochDay()
    }

    @JvmStatic
    fun truncMonth(epochDay: Long): Long = LocalDate.ofEpochDay(epochDay).withDayOfMonth(1).toEpochDay()

    /** the Monday on or before [epochDay] - epoch day 0 is a Thursday */
    @JvmStatic
    fun truncWeek(epochDay: Long): Long = epochDay - floorMod(epochDay + 3, 7L)
}
