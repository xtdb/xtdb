package xtdb.bitemporal

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import com.carrotsearch.hppc.LongArrayList.from as longs
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG

internal class PolygonTest {
    private lateinit var polygon: Polygon
    private lateinit var ceiling: Ceiling

    @BeforeEach
    fun setUp() {
        polygon = Polygon()
        ceiling = Ceiling()
    }

    private fun applyEvent(sysFrom: Long, inPolygon: Polygon) {
        polygon.calculateFor(ceiling, inPolygon)

        ceiling.applyLog(
            sysFrom,
            inPolygon.getValidFrom(0),
            inPolygon.getValidTo(inPolygon.validTimeRangeCount - 1)
        )
    }

    private fun event(validFrom: Long, validTo: Long) = Polygon(longs(validFrom, validTo), longs(MAX_LONG))

    @Test
    fun testCalculateForEmptyCeiling() {
        applyEvent(0, event(2, 3))
        assertEquals(longs(2, 3), polygon.validTimes)
        assertEquals(longs(MAX_LONG), polygon.sysTimeCeilings)
    }

    @Test
    fun startsBeforeNoOverlap() {
        applyEvent(1, event(2005, 2009))
        assertEquals(longs(2005, 2009), polygon.validTimes)
        assertEquals(longs(MAX_LONG), polygon.sysTimeCeilings)

        applyEvent(0, event(2010, 2020))
        assertEquals(longs(2010, 2020), polygon.validTimes)
        assertEquals(longs(MAX_LONG), polygon.sysTimeCeilings)
    }

    @Test
    fun startsBeforeAndOverlaps() {
        applyEvent(1, event(2010, 2020))
        assertEquals(longs(2010, 2020), polygon.validTimes)
        assertEquals(longs(MAX_LONG), polygon.sysTimeCeilings)

        applyEvent(0, event(2015, 2025))
        assertEquals(longs(2015, 2020, 2025), polygon.validTimes)
        assertEquals(longs(1, MAX_LONG), polygon.sysTimeCeilings)
    }

    @Test
    fun startsEquallyAndOverlaps() {
        applyEvent(1, event(2010, 2020))
        assertEquals(longs(2010, 2020), polygon.validTimes)
        assertEquals(longs(MAX_LONG), polygon.sysTimeCeilings)

        applyEvent(0, event(2010, 2025))
        assertEquals(longs(2010, 2020, 2025), polygon.validTimes)
        assertEquals(longs(1, MAX_LONG), polygon.sysTimeCeilings)
    }

    @Test
    fun newerPeriodCompletelyCovered() {
        applyEvent(1, event(2015, 2020))
        applyEvent(0, event(2010, 2025))

        assertEquals(longs(2010, 2015, 2020, 2025), polygon.validTimes)
        assertEquals(longs(MAX_LONG, 1, MAX_LONG), polygon.sysTimeCeilings)
    }

    @Test
    fun olderPeriodCompletelyCovered() {
        applyEvent(1, event(2010, 2025))
        applyEvent(0, event(2010, 2020))

        assertEquals(longs(2010, 2020), polygon.validTimes)
        assertEquals(longs(1), polygon.sysTimeCeilings)
    }

    @Test
    fun periodEndsEquallyAndOverlaps() {
        applyEvent(1, event(2015, 2025))
        applyEvent(0, event(2010, 2025))

        assertEquals(longs(2010, 2015, 2025), polygon.validTimes)
        assertEquals(longs(MAX_LONG, 1), polygon.sysTimeCeilings)
    }

    @Test
    fun periodEndsAfterAndOverlaps() {
        applyEvent(1, event(2015, 2025))
        applyEvent(0, event(2010, 2020))

        assertEquals(longs(2010, 2015, 2020), polygon.validTimes)
        assertEquals(longs(MAX_LONG, 1), polygon.sysTimeCeilings)
    }

    @Test
    fun periodStartsBeforeAndTouches() {
        applyEvent(1, event(2005, 2010))
        applyEvent(0, event(2010, 2020))

        assertEquals(longs(2010, 2020), polygon.validTimes)
        assertEquals(longs(MAX_LONG), polygon.sysTimeCeilings)
    }

    @Test
    fun periodStartsAfterAndTouches() {
        applyEvent(1, event(2010, 2020))
        applyEvent(0, event(2005, 2010))

        assertEquals(longs(2005, 2010), polygon.validTimes)
        assertEquals(longs(MAX_LONG), polygon.sysTimeCeilings)
    }

    @Test
    fun periodStartsAfterAndDoesNotOverlap() {
        applyEvent(1, event(2010, 2020))
        applyEvent(0, event(2005, 2009))

        assertEquals(longs(2005, 2009), polygon.validTimes)
        assertEquals(longs(MAX_LONG), polygon.sysTimeCeilings)
    }

    @Test
    fun testCalculatingFromMultiRangePolygon() {
        applyEvent(5, event(2012, 2015))

        applyEvent(
            1,
            Polygon(
                longs(2005, 2009, 2010, MAX_LONG),
                longs(4, 3, MAX_LONG)
            )
        )

        assertEquals(longs(2005, 2009, 2010, 2012, 2015, MAX_LONG), polygon.validTimes)
        assertEquals(longs(4, 3, MAX_LONG, 5, MAX_LONG), polygon.sysTimeCeilings)
    }

    @Test
    fun testMultiRangePolygonMeetsNewEvent() {
        applyEvent(5, event(2010, 2015))

        applyEvent(
            1,
            Polygon(
                longs(2005, 2009, 2010, MAX_LONG),
                longs(4, 3, MAX_LONG)
            )
        )
        assertEquals(longs(2005, 2009, 2010, 2015, MAX_LONG), polygon.validTimes)
        assertEquals(longs(4, 3, 5, MAX_LONG), polygon.sysTimeCeilings)
    }

    @Test
    fun testLaterEventDoesntChangeSupersededEvent() {
        applyEvent(5, event(2010, 2015))

        applyEvent(
            1,
            Polygon(
                longs(2005, 2009, MAX_LONG),
                longs(MAX_LONG, 3)
            )
        )

        assertEquals(longs(2005, 2009, MAX_LONG), polygon.validTimes)
        assertEquals(longs(MAX_LONG, 3), polygon.sysTimeCeilings)
    }
}
