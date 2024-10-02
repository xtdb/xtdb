package xtdb.bitemporal

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.util.SkipList
import com.carrotsearch.hppc.LongArrayList.from as longs
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG
import kotlin.Long.Companion.MIN_VALUE as MIN_LONG

internal class PolygonTest {
    private lateinit var polygon: Polygon
    private lateinit var ceiling: Ceiling

    @BeforeEach
    fun setUp() {
        polygon = Polygon()
        ceiling = Ceiling()
    }

    private fun applyEvent(sysFrom: Long, validFrom: Long, validTo: Long) {
        polygon.calculateFor(ceiling, validFrom, validTo)

        ceiling.applyLog(
            sysFrom,
            validFrom,
            validTo
        )
    }

    @Test
    fun testCalculateForEmptyCeiling() {
        applyEvent(0, 2, 3)
        assertEquals(longs(2, 3), polygon.validTimes)
        assertEquals(longs(MAX_LONG), polygon.sysTimeCeilings)
    }

    @Test
    fun startsBeforeNoOverlap() {
        applyEvent(1, 2005, 2009)
        assertEquals(longs(2005, 2009), polygon.validTimes)
        assertEquals(longs(MAX_LONG), polygon.sysTimeCeilings)

        applyEvent(0, 2010, 2020)
        assertEquals(longs(2010, 2020), polygon.validTimes)
        assertEquals(longs(MAX_LONG), polygon.sysTimeCeilings)
    }

    @Test
    fun startsBeforeAndOverlaps() {
        applyEvent(1, 2010, 2020)
        assertEquals(longs(2010, 2020), polygon.validTimes)
        assertEquals(longs(MAX_LONG), polygon.sysTimeCeilings)

        applyEvent(0, 2015, 2025)
        assertEquals(longs(2015, 2020, 2025), polygon.validTimes)
        assertEquals(longs(1, MAX_LONG), polygon.sysTimeCeilings)
    }

    @Test
    fun startsEquallyAndOverlaps() {
        applyEvent(1, 2010, 2020)
        assertEquals(longs(2010, 2020), polygon.validTimes)
        assertEquals(longs(MAX_LONG), polygon.sysTimeCeilings)

        applyEvent(0, 2010, 2025)
        assertEquals(longs(2010, 2020, 2025), polygon.validTimes)
        assertEquals(longs(1, MAX_LONG), polygon.sysTimeCeilings)
    }

    @Test
    fun newerPeriodCompletelyCovered() {
        applyEvent(1, 2015, 2020)
        applyEvent(0, 2010, 2025)

        assertEquals(longs(2010, 2015, 2020, 2025), polygon.validTimes)
        assertEquals(longs(MAX_LONG, 1, MAX_LONG), polygon.sysTimeCeilings)
    }

    @Test
    fun olderPeriodCompletelyCovered() {
        applyEvent(1, 2010, 2025)
        applyEvent(0, 2010, 2020)

        assertEquals(longs(2010, 2020), polygon.validTimes)
        assertEquals(longs(1), polygon.sysTimeCeilings)
    }

    @Test
    fun periodEndsEquallyAndOverlaps() {
        applyEvent(1, 2015, 2025)
        applyEvent(0, 2010, 2025)

        assertEquals(longs(2010, 2015, 2025), polygon.validTimes)
        assertEquals(longs(MAX_LONG, 1), polygon.sysTimeCeilings)
    }

    @Test
    fun periodEndsAfterAndOverlaps() {
        applyEvent(1, 2015, 2025)
        applyEvent(0, 2010, 2020)

        assertEquals(longs(2010, 2015, 2020), polygon.validTimes)
        assertEquals(longs(MAX_LONG, 1), polygon.sysTimeCeilings)
    }

    @Test
    fun periodStartsBeforeAndTouches() {
        applyEvent(1, 2005, 2010)
        applyEvent(0, 2010, 2020)

        assertEquals(longs(2010, 2020), polygon.validTimes)
        assertEquals(longs(MAX_LONG), polygon.sysTimeCeilings)
    }

    @Test
    fun periodStartsAfterAndTouches() {
        applyEvent(1, 2010, 2020)
        applyEvent(0, 2005, 2010)

        assertEquals(longs(2005, 2010), polygon.validTimes)
        assertEquals(longs(MAX_LONG), polygon.sysTimeCeilings)
    }

    @Test
    fun periodStartsAfterAndDoesNotOverlap() {
        applyEvent(1, 2010, 2020)
        applyEvent(0, 2005, 2009)

        assertEquals(longs(2005, 2009), polygon.validTimes)
        assertEquals(longs(MAX_LONG), polygon.sysTimeCeilings)
    }

    @Test
    fun testTimeSeries() {
        ceiling.applyLog(10, 10, 12)
        ceiling.applyLog(8, 8, 10)
        ceiling.applyLog(6, 6, 8)

        assertEquals(SkipList.from(MAX_LONG, 12, 10, 8, 6, MIN_LONG), ceiling.validTimes)
        assertEquals(SkipList.from(MAX_LONG, 10, 8, 6, MAX_LONG), ceiling.sysTimeCeilings)

        applyEvent(4, 4, 6)
        assertEquals(longs(4, 6), polygon.validTimes)
        assertEquals(longs(MAX_LONG), polygon.sysTimeCeilings)
    }
}
