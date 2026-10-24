@file:OptIn(InternalApi::class)

package xtdb.time

import io.kotest.property.Arb
import io.kotest.property.arbitrary.bind
import io.kotest.property.arbitrary.choice
import io.kotest.property.arbitrary.element
import io.kotest.property.arbitrary.filter
import io.kotest.property.arbitrary.flatMap
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.long
import io.kotest.property.arbitrary.map
import io.kotest.property.checkAll
import kotlinx.coroutines.test.runTest
import org.apache.arrow.vector.PeriodDuration
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.InternalApi
import java.time.DayOfWeek
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.Period
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime

class TemporalMathTest {
    companion object {
        private const val ITERATIONS = 10_000

        private val zones: Arb<ZoneId> = Arb.choice(
            Arb.element(ZoneId.getAvailableZoneIds().sorted().map(ZoneId::of)),
            Arb.int(-18 * 3600..18 * 3600).map(ZoneOffset::ofTotalSeconds)
        )

        private val epochDays = Arb.long(LocalDate.MIN.toEpochDay()..LocalDate.MAX.toEpochDay())

        // a few days inside LocalDateTime's range, so a reading moved by a day or two still fits
        private val maxEpochSecond = LocalDateTime.MAX.toEpochSecond(ZoneOffset.UTC) - 3 * 86_400
        private val epochSeconds = Arb.long(-maxEpochSecond..maxEpochSecond)

        private val units = Arb.element(1L, 1_000L, 1_000_000L, 1_000_000_000L)

        // toLocal's domain: the instant has to fit a ZonedDateTime, which only binds at seconds, and its local reading a long
        private fun timestamps(hz: Long): Arb<Long> =
            if (hz == 1L) epochSeconds else Arb.long(-(Long.MAX_VALUE - 18L * 3600 * hz)..Long.MAX_VALUE - 18L * 3600 * hz)

        private val unitTimestamps = units.flatMap { hz -> timestamps(hz).map { hz to it } }

        private val intervals = Arb.bind(
            Arb.int(-1000..1000), Arb.int(-3000..3000), Arb.int(-100_000..100_000),
            Arb.long(-10_000_000_000L..10_000_000_000L), Arb.long(0L..999_999_999L)
        ) { years, months, days, secs, nanos -> PeriodDuration(Period.of(years, months, days), Duration.ofSeconds(secs, nanos)) }

        // far enough inside LocalDate's range, and a micros timestamp's, that no interval above can push a result out of it
        private val intervalEpochDays = Arb.long(LocalDate.MIN.toEpochDay() + 1_000_000..LocalDate.MAX.toEpochDay() - 1_000_000)
        private val intervalMicros = Arb.long(Long.MIN_VALUE + 100_000_000_000_000_000..Long.MAX_VALUE - 100_000_000_000_000_000)

        // ±250 years, so a pair of them fits a long at every unit
        private val ageSeconds = Arb.long(-8_000_000_000L..8_000_000_000L)
        private val nanosOfSecond = Arb.long(0L..999_999_999L)

        private const val SERIES_ITERATIONS = 1_000

        private val strides = Arb.bind(Arb.int(0..2), Arb.int(0..2), Arb.long(0L..90_000L), Arb.long(0L..999_999_999L)) { months, days, secs, nanos ->
            PeriodDuration(Period.of(0, months, days), Duration.ofSeconds(secs, nanos))
        }.filter { !(it.period.isZero && it.duration.isZero) }
    }

    private fun assertLocalReading(zone: ZoneId, ts: Long, hz: Long) {
        val ldt = ZonedDateTime.ofInstant(
            Instant.ofEpochSecond(Math.floorDiv(ts, hz), Math.floorMod(ts, hz) * (1_000_000_000 / hz)), zone
        ).toLocalDateTime()
        val local = TemporalMath.toLocal(ts, hz, ZoneOffsets.of(zone))
        assertEquals(ldt.toLocalDate().toEpochDay(), TemporalMath.epochDay(local, hz), "epoch day $zone $ts/$hz")
        assertEquals(ldt.toLocalTime().toSecondOfDay(), TemporalMath.secondOfDay(local, hz), "second of day $zone $ts/$hz")
        assertEquals(ldt.toLocalTime().toNanoOfDay() / (1_000_000_000 / hz), TemporalMath.unitOfDay(local, hz), "unit of day $zone $ts/$hz")
    }

    private fun epochSecondsAroundTransitions(zone: ZoneId): List<Long> {
        val out = mutableListOf<Long>()
        var t = Instant.ofEpochSecond(-3_000_000_000)
        repeat(400) {
            val tr = zone.rules.nextTransition(t) ?: return out
            for (delta in longArrayOf(-90_000, -3600, -1, 0, 1, 1800, 3600, 90_000)) out += tr.instant.epochSecond + delta
            t = tr.instant
        }
        return out
    }

    @Test
    fun `local wall-clock time matches ZonedDateTime in every zone`() {
        for (id in ZoneId.getAvailableZoneIds()) {
            val zone = ZoneId.of(id)
            for (s in epochSecondsAroundTransitions(zone)) assertLocalReading(zone, s, 1)
        }
    }

    @Test
    fun `local wall-clock time matches ZonedDateTime for any instant, unit and zone`() = runTest {
        checkAll(ITERATIONS, zones, unitTimestamps) { zone, (hz, ts) -> assertLocalReading(zone, ts, hz) }
    }

    private fun localSecond(zone: ZoneId, s: Long) =
        ZonedDateTime.ofInstant(Instant.ofEpochSecond(s), zone).toLocalDateTime().toEpochSecond(ZoneOffset.UTC)

    private fun assertResolves(zone: ZoneId, s: Long, probe: Long) {
        val offsets = ZoneOffsets.of(zone)
        val zdt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(s), zone)
        val ldt = LocalDateTime.ofEpochSecond(probe, 0, ZoneOffset.UTC)
        assertEquals(zdt.with(ldt).toEpochSecond(), TemporalMath.fromLocalNear(probe, 1, offsets, s), "$zone $ldt near $zdt")
        assertEquals(ldt.atZone(zone).toEpochSecond(), TemporalMath.fromLocal(probe, 1, offsets), "$zone $ldt at zone")
    }

    private fun assertStartOfDay(zone: ZoneId, d: LocalDate) =
        assertEquals(d.atStartOfDay(zone).toEpochSecond(), TemporalMath.startOfDay(d.toEpochDay(), 1, ZoneOffsets.of(zone)), "$zone start of $d")

    @Test
    fun `local readings resolve as ZonedDateTime does in every zone`() {
        for (id in ZoneId.getAvailableZoneIds()) {
            val zone = ZoneId.of(id)
            for (s in epochSecondsAroundTransitions(zone)) {
                val local = localSecond(zone, s)
                for (probe in longArrayOf(local, local - 3600, local - 1800, local + 1800, local + 3600, local - 86_400))
                    assertResolves(zone, s, probe)

                val day = LocalDate.ofEpochDay(Math.floorDiv(local, 86_400L))
                for (d in listOf(day.minusDays(1), day, day.plusDays(1))) assertStartOfDay(zone, d)
            }
        }
    }

    @Test
    fun `local readings resolve as ZonedDateTime does for any instant and zone`() = runTest {
        checkAll(ITERATIONS, zones, epochSeconds, Arb.long(-2 * 86_400L..2 * 86_400L)) { zone, s, delta ->
            val probe = localSecond(zone, s) + delta
            assertResolves(zone, s, probe)
            assertStartOfDay(zone, LocalDate.ofEpochDay(Math.floorDiv(probe, 86_400L)))
        }
    }

    @Test
    fun `truncations match LocalDate across its whole range`() = runTest {
        checkAll(ITERATIONS, epochDays) { d ->
            val ld = LocalDate.ofEpochDay(d)
            for (m in longArrayOf(1, 10, 100, 1000))
                assertEquals(LocalDate.of((ld.year / m.toInt()) * m.toInt(), 1, 1).toEpochDay(), TemporalMath.truncYear(d, m), "trunc year/$m $ld")
            assertEquals(ld.withMonth(ld.month.firstMonthOfQuarter().value).withDayOfMonth(1).toEpochDay(), TemporalMath.truncQuarter(d), "trunc quarter $ld")
            assertEquals(ld.withDayOfMonth(1).toEpochDay(), TemporalMath.truncMonth(d), "trunc month $ld")
            assertEquals(ld.with(DayOfWeek.MONDAY).toEpochDay(), TemporalMath.truncWeek(d), "trunc week $ld")
        }
    }

    private val micros = 1_000_000L

    private fun Instant.toMicros() = epochSecond * micros + nano / 1000
    private fun Long.microsToInstant() = Instant.ofEpochSecond(Math.floorDiv(this, micros), Math.floorMod(this, micros) * 1000)
    private fun LocalDateTime.toMicros() = toInstant(ZoneOffset.UTC).toMicros()
    private fun Long.microsToLocal() = LocalDateTime.ofEpochSecond(Math.floorDiv(this, micros), (Math.floorMod(this, micros) * 1000).toInt(), ZoneOffset.UTC)

    private fun assertTzPlusInterval(zone: ZoneId, ts: Long, i: PeriodDuration) {
        val offsets = ZoneOffsets.of(zone)
        val zdt = ZonedDateTime.ofInstant(ts.microsToInstant(), zone)
        assertEquals(zdt.plus(i.period).plus(i.duration).toInstant().toMicros(), TemporalMath.tzPlusInterval(ts, micros, offsets, i, 1), "$zdt + $i")
        assertEquals(zdt.minus(i.period).minus(i.duration).toInstant().toMicros(), TemporalMath.tzPlusInterval(ts, micros, offsets, i, -1), "$zdt - $i")
    }

    @Test
    fun `date and local timestamp interval arithmetic matches java time`() = runTest {
        checkAll(ITERATIONS, intervalEpochDays, intervalMicros, intervals) { d, ts, i ->
            val ld = LocalDate.ofEpochDay(d)
            assertEquals(ld.plus(i.period).toEpochDay(), TemporalMath.datePlusInterval(d, i, 1), "$ld + $i")
            assertEquals(ld.minus(i.period).toEpochDay(), TemporalMath.datePlusInterval(d, i, -1), "$ld - $i")

            val ldt = ts.microsToLocal()
            assertEquals(ldt.plus(i.period).plus(i.duration).toMicros(), TemporalMath.localPlusInterval(ts, micros, i, 1), "$ldt + $i")
            assertEquals(ldt.minus(i.period).minus(i.duration).toMicros(), TemporalMath.localPlusInterval(ts, micros, i, -1), "$ldt - $i")
        }
    }

    @Test
    fun `timestamp-tz interval arithmetic matches ZonedDateTime for any instant and zone`() = runTest {
        checkAll(ITERATIONS, zones, intervalMicros, intervals) { zone, ts, i -> assertTzPlusInterval(zone, ts, i) }
    }

    @Test
    fun `timestamp-tz interval arithmetic matches ZonedDateTime around every transition`() {
        val fixed = listOf(
            PeriodDuration(Period.ofDays(1), Duration.ZERO),
            PeriodDuration(Period.ZERO, Duration.ofHours(24)),
            PeriodDuration(Period.ofMonths(1), Duration.ZERO),
            PeriodDuration(Period.of(0, 1, -1), Duration.ofMinutes(-90)),
            PeriodDuration(Period.ZERO, Duration.ofNanos(500)),
        )
        for (id in ZoneId.getAvailableZoneIds()) {
            val zone = ZoneId.of(id)
            for (s in epochSecondsAroundTransitions(zone))
                for (i in fixed) assertTzPlusInterval(zone, s * micros + 250_000, i)
        }
    }

    private fun javaTimeAge(end: LocalDateTime, start: LocalDateTime): PeriodDuration {
        val period = Period.between(start.toLocalDate(), end.toLocalDate())
        val adjustment = when {
            period.isNegative && start.toLocalTime().isBefore(end.toLocalTime()) -> 1L
            !period.isNegative && period.days != 0 && start.toLocalTime().isAfter(end.toLocalTime()) -> -1L
            else -> 0L
        }
        val duration = Duration.between(start, end)
        return PeriodDuration(period.plusDays(adjustment), duration.minusDays(duration.toDays()))
    }

    private fun ts(sec: Long, nano: Long, hz: Long) = sec * hz + nano / (1_000_000_000 / hz)

    private fun assertAge(endHz: Long, endSec: Long, endNano: Long, startHz: Long, startSec: Long, startNano: Long) {
        val endTs = ts(endSec, endNano, endHz)
        val startTs = ts(startSec, startNano, startHz)
        fun ldt(ts: Long, hz: Long) =
            LocalDateTime.ofEpochSecond(Math.floorDiv(ts, hz), (Math.floorMod(ts, hz) * (1_000_000_000 / hz)).toInt(), ZoneOffset.UTC)

        assertEquals(javaTimeAge(ldt(endTs, endHz), ldt(startTs, startHz)), TemporalMath.ageInterval(endTs, endHz, startTs, startHz), "age $endTs/$endHz $startTs/$startHz")
    }

    @Test
    fun `age matches its java time definition for any two timestamps`() = runTest {
        checkAll(ITERATIONS, units, ageSeconds, nanosOfSecond, units, ageSeconds, nanosOfSecond) { endHz, endSec, endNano, startHz, startSec, startNano ->
            assertAge(endHz, endSec, endNano, startHz, startSec, startNano)
        }
    }

    @Test
    fun `age matches its java time definition for timestamps within 90 days of each other`() = runTest {
        checkAll(ITERATIONS, units, ageSeconds, nanosOfSecond, units, Arb.long(-90L * 86_400..90L * 86_400), nanosOfSecond) { endHz, endSec, endNano, startHz, delta, startNano ->
            assertAge(endHz, endSec, endNano, startHz, endSec + delta, startNano)
        }
    }

    private fun <T : Comparable<T>> iterateSeries(from: T, to: T, inclusive: Boolean, step: (T) -> T): List<T> =
        generateSequence(from, step).takeWhile { it < to || (inclusive && it.compareTo(to) == 0) }.toList()

    @Test
    fun `date series clamp to month end from the previous step, as iterated LocalDate does`() {
        val jan31 = LocalDate.of(2023, 1, 31)
        assertEquals(
            iterateSeries(jan31, jan31.plusYears(2), true) { it.plusMonths(1) }.map { it.toEpochDay() },
            TemporalMath.dateSeries(jan31.toEpochDay(), jan31.plusYears(2).toEpochDay(), 1, true).toList()
        )
    }

    // no stride is shorter than this, so a span of `n` of them holds at most `n + 1` elements
    private fun PeriodDuration.minNanos() =
        (period.months * 28L + period.days) * 86_400_000_000_000 + duration.seconds * 1_000_000_000 + duration.nano

    @Test
    fun `timestamp series step as iterated java time does`() = runTest {
        val nanos = 1_000_000_000L
        checkAll(
            SERIES_ITERATIONS, zones, Arb.long(-7_000_000_000L..7_000_000_000L), Arb.long(0L..999_999L), strides, Arb.int(0..200), Arb.element(true, false)
        ) { zone, fromSec, fromMicros, stride, steps, inclusive ->
            val from = fromSec * micros + fromMicros
            val to = from + stride.minNanos() * steps / 1000

            val ldtFrom = from.microsToLocal()
            val ldtTo = to.microsToLocal()
            assertEquals(
                iterateSeries(ldtFrom, ldtTo, inclusive) { it.plus(stride.period).plus(stride.duration) }
                    .map { val i = it.toInstant(ZoneOffset.UTC); i.epochSecond * nanos + i.nano },
                TemporalMath.localSeries(from, micros, to, micros, stride, nanos, inclusive).toList(),
                "local $ldtFrom..$ldtTo by $stride"
            )

            assertEquals(
                iterateSeries(ZonedDateTime.ofInstant(from.microsToInstant(), zone), ZonedDateTime.ofInstant(to.microsToInstant(), zone), inclusive) {
                    it.plus(stride.period).plus(stride.duration)
                }.map { it.toInstant().toMicros() },
                TemporalMath.tzSeries(from, micros, to, micros, ZoneOffsets.of(zone), stride, micros, inclusive).toList(),
                "tz $zone from $from to $to by $stride"
            )
        }
    }
}
