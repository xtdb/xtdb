@file:OptIn(InternalApi::class)

package xtdb.time

import io.kotest.property.Arb
import io.kotest.property.arbitrary.choice
import io.kotest.property.arbitrary.element
import io.kotest.property.arbitrary.flatMap
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.long
import io.kotest.property.arbitrary.map
import io.kotest.property.checkAll
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.InternalApi
import java.time.DayOfWeek
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
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
    }

    private fun assertLocalReading(zone: ZoneId, ts: Long, hz: Long) {
        val ldt = ZonedDateTime.ofInstant(
            Instant.ofEpochSecond(Math.floorDiv(ts, hz), Math.floorMod(ts, hz) * (1_000_000_000 / hz)), zone
        ).toLocalDateTime()
        val local = TemporalMath.toLocal(ts, hz, ZoneOffsets.of(zone))
        assertEquals(ldt.toLocalDate().toEpochDay(), TemporalMath.epochDay(local, hz), "epoch day $zone $ts/$hz")
        assertEquals(ldt.toLocalTime().toSecondOfDay(), TemporalMath.secondOfDay(local, hz), "second of day $zone $ts/$hz")
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

    private fun assertResolvesNear(zone: ZoneId, s: Long, probe: Long) {
        val zdt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(s), zone)
        val ldt = LocalDateTime.ofEpochSecond(probe, 0, ZoneOffset.UTC)
        assertEquals(zdt.with(ldt).toEpochSecond(), TemporalMath.fromLocalNear(probe, 1, ZoneOffsets.of(zone), s), "$zone $ldt near $zdt")
    }

    @Test
    fun `local readings resolve near the original instant as ZonedDateTime with does in every zone`() {
        for (id in ZoneId.getAvailableZoneIds()) {
            val zone = ZoneId.of(id)
            for (s in epochSecondsAroundTransitions(zone)) {
                val local = localSecond(zone, s)
                for (probe in longArrayOf(local, local - 3600, local - 1800, local + 1800, local + 3600, local - 86_400))
                    assertResolvesNear(zone, s, probe)
            }
        }
    }

    @Test
    fun `local readings resolve near the original instant as ZonedDateTime with does for any instant and zone`() = runTest {
        checkAll(ITERATIONS, zones, epochSeconds, Arb.long(-2 * 86_400L..2 * 86_400L)) { zone, s, delta ->
            assertResolvesNear(zone, s, localSecond(zone, s) + delta)
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
}
