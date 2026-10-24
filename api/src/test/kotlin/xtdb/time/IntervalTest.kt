package xtdb.time

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration

class IntervalTest {

    @Test
    fun `months print as normalised years and months, days and time as they are`() {
        assertEquals("P", Interval.ZERO.toString())
        assertEquals("P2Y", Interval(24, 0, 0).toString())
        assertEquals("P1Y10M", Interval(22, 0, 0).toString())
        assertEquals("P-1Y-10M", Interval(-22, 0, 0).toString())
        assertEquals("P10M", Interval(10, 0, 0).toString())
        assertEquals("P400D", Interval(0, 400, 0).toString())
        assertEquals("P1Y-3D", Interval(12, -3, 0).toString())
        assertEquals("PT26H", Interval(0, 0, Duration.ofHours(26).toNanos()).toString())
        assertEquals("P-1Y-10M3DT-4H", Interval(-22, 3, Duration.ofHours(-4).toNanos()).toString())
    }

    @Test
    fun `normalised string parses back to the same interval`() {
        listOf(
            Interval(24, 0, 0),
            Interval(22, 0, 0),
            Interval(-22, 0, 0),
            Interval(-10, 0, 0),
            Interval(12, -3, 0),
            Interval(-22, 3, Duration.ofHours(-4).toNanos()),
            Interval(-22, -3, Duration.ofMinutes(90).toNanos()),
        ).forEach { assertEquals(it, it.toString().asInterval(), "$it") }
    }

    @Test
    fun `years beyond Int months still print`() {
        assertEquals("P2147483647Y", Interval.toIsoString(Int.MAX_VALUE * 12L, 0, Duration.ZERO))
    }
}
