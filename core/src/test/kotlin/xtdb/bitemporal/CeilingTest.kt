package xtdb.bitemporal

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import com.carrotsearch.hppc.LongArrayList as longs
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG
import kotlin.Long.Companion.MIN_VALUE as MIN_LONG

internal class CeilingTest {
    private lateinit var ceiling: Ceiling

    @BeforeEach
    fun setUp() {
        ceiling = Ceiling()
    }

    @Test
    fun testAppliesLogs() {
        assertEquals(longs.from(MIN_LONG, MAX_LONG), ceiling.validTimes)
        assertEquals(longs.from(MAX_LONG), ceiling.sysTimeCeilings)

        ceiling.applyLog(4, 4, MAX_LONG)
        assertEquals(longs.from(MIN_LONG, 4, MAX_LONG), ceiling.validTimes)
        assertEquals(longs.from(MAX_LONG, 4), ceiling.sysTimeCeilings)

        // lower the whole ceiling
        ceiling.applyLog(3, 2, MAX_LONG)
        assertEquals(longs.from(MIN_LONG, 2, MAX_LONG), ceiling.validTimes)
        assertEquals(longs.from(MAX_LONG, 3), ceiling.sysTimeCeilings)

        // lower part of the ceiling
        ceiling.applyLog(2, 1, 4)
        assertEquals(longs.from(MIN_LONG, 1, 4, MAX_LONG), ceiling.validTimes)
        assertEquals(longs.from(MAX_LONG, 2, 3), ceiling.sysTimeCeilings)

        // replace a range exactly
        ceiling.applyLog(1, 1, 4)
        assertEquals(longs.from(MIN_LONG, 1, 4, MAX_LONG), ceiling.validTimes)
        assertEquals(longs.from(MAX_LONG, 1, 3), ceiling.sysTimeCeilings)

        // replace the whole middle section
        ceiling.applyLog(0, 0, 6)
        assertEquals(longs.from(MIN_LONG, 0, 6, MAX_LONG), ceiling.validTimes)
        assertEquals(longs.from(MAX_LONG, 0, 3), ceiling.sysTimeCeilings)
    }
}
