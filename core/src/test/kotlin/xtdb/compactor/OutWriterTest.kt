package xtdb.compactor

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.time.InstantUtil.asMicros
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime

class OutWriterTest {
    private fun zdt(
        year: Int, month: Int = 1, day: Int = 1,
        hour: Int = 0, minute: Int = 0,
        tz: ZoneId = ZoneOffset.UTC
    ): ZonedDateTime = ZonedDateTime.of(year, month, day, hour, minute, 0, 0, tz)

    @Test
    fun `test recency partitioning`() {
        assertEquals(
            LocalDate.of(2025, 3, 10),
            zdt(2025, 3, 10).toInstant().asMicros.toPartition(),
            "Monday midnight"
        )

        assertEquals(
            LocalDate.of(2025, 3, 17),
            zdt(2025, 3, 10, 4).toInstant().asMicros.toPartition(),
            "Monday not midnight"
        )

        assertEquals(
            LocalDate.of(2025, 3, 17),
            zdt(2025, 3, 11).toInstant().asMicros.toPartition(),
            "Tuesday"
        )

        assertEquals(
            LocalDate.of(2025, 3, 17),
            zdt(2025, 3, 17).minusNanos(1).toInstant().asMicros.toPartition(),
            "Sunday just before midnight"
        )
    }
}