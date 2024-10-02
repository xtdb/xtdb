package xtdb.bitemporal

import com.carrotsearch.hppc.LongArrayList
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.util.SkipList
import kotlin.Long.Companion.MAX_VALUE
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
    fun testReverseLinearSearch() {
        val list = longs.from(10, 8, 6, 4, 2)
        assertEquals(0, list.reverseLinearSearch(10))
        assertEquals(2, list.reverseLinearSearch(6))
        assertEquals(4, list.reverseLinearSearch(2))
        assertEquals(-2, list.reverseLinearSearch(9))
        assertEquals(-1, list.reverseLinearSearch(11))
        assertEquals(-5, list.reverseLinearSearch(3))
        assertEquals(-6, list.reverseLinearSearch(1))
    }

    @Test
    fun testBinarySearch() {
        val list = longs.from(10, 8, 6, 4, 2)
        assertEquals(0, list.binarySearch(10))
        assertEquals(2, list.binarySearch(6))
        assertEquals(4, list.binarySearch(2))
        assertEquals(-2, list.binarySearch(9))
        assertEquals(-1, list.binarySearch(11))
        assertEquals(-5, list.binarySearch(3))
        assertEquals(-6, list.binarySearch(1))
    }

    @Test
    fun testGetCeilingIndex() {
        val list = SkipList.from(10L, 8L, 6L, 4L, 2L)
        // the ceilings part shouldn't be used in this test
        val ceiling = Ceiling(list, SkipList())
        assertEquals(0, ceiling.getCeilingIndex(1))
        assertEquals(0, ceiling.getCeilingIndex(2))
        assertEquals(4, ceiling.getCeilingIndex(10))
        assertEquals(4, ceiling.getCeilingIndex(11))
        assertEquals(1, ceiling.getCeilingIndex(5))
    }

    @Test
    fun testAppliesLogs() {
        assertEquals(SkipList.from(MAX_LONG, MIN_LONG), ceiling.validTimes)
        assertEquals(SkipList.from(MAX_LONG), ceiling.sysTimeCeilings)

        ceiling.applyLog(4, 4, MAX_LONG)
        assertEquals(SkipList.from(MAX_LONG, 4, MIN_LONG), ceiling.validTimes)
        assertEquals(SkipList.from(4, MAX_LONG), ceiling.sysTimeCeilings)

        // lower the whole ceiling
        ceiling.applyLog(3, 2, MAX_LONG)
        assertEquals(SkipList.from(MAX_LONG, 2, MIN_LONG), ceiling.validTimes)
        assertEquals(SkipList.from(3, MAX_LONG), ceiling.sysTimeCeilings)

        // lower part of the ceiling
        ceiling.applyLog(2, 1, 4)
        assertEquals(SkipList.from(MAX_LONG, 4, 1, MIN_LONG), ceiling.validTimes)
        assertEquals(SkipList.from(3, 2, MAX_LONG), ceiling.sysTimeCeilings)

        // replace a range exactly
        ceiling.applyLog(1, 1, 4)
        assertEquals(SkipList.from(MAX_LONG, 4, 1, MIN_LONG), ceiling.validTimes)
        assertEquals(SkipList.from(3, 1, MAX_LONG), ceiling.sysTimeCeilings)

        // replace the whole middle section
        ceiling.applyLog(0, 0, 6)
        assertEquals(SkipList.from(MAX_LONG, 6, 0, MIN_LONG), ceiling.validTimes)
        assertEquals(SkipList.from(3, 0, MAX_LONG), ceiling.sysTimeCeilings)
    }

    @Test
    fun `test replace within a range`() {
        ceiling.applyLog(4, 4, 6)
        assertEquals(SkipList.from(MAX_LONG, 6, 4, MIN_LONG), ceiling.validTimes)
        assertEquals(SkipList.from(MAX_LONG, 4, MAX_LONG), ceiling.sysTimeCeilings)
    }
}
