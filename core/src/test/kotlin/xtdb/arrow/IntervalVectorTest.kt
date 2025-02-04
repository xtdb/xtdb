package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.types.IntervalDayTime
import xtdb.types.IntervalMonthDayNano
import java.time.Duration
import java.time.Period

class IntervalVectorTest {
    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    @Test
    fun testIntervalMonthDayNanoReadWrite() {
        IntervalMonthDayNanoVector(allocator, "imdnv", false).use {
            iv ->
            iv.writeObject(IntervalMonthDayNano(Period.ofMonths(0), Duration.ofHours(1)))
            iv.writeObject(IntervalMonthDayNano(Period.ofMonths(1), Duration.ofHours(0)))

            assertEquals(2, iv.valueCount)
            assertEquals(IntervalMonthDayNano(Period.ofMonths(0), Duration.ofHours(1)), iv.getObject(0))
            assertEquals(IntervalMonthDayNano(Period.ofMonths(1), Duration.ofHours(0)), iv.getObject(1))
        }
    }

    @Test
    fun testIntervalDayTimeReadWrite() {
        IntervalDayTimeVector(allocator, "idtv", false).use {
                iv ->
            iv.writeObject(IntervalDayTime(Period.ofDays(0), Duration.ofHours(1)))
            iv.writeObject(IntervalDayTime(Period.ofDays(1), Duration.ofHours(0)))

            assertEquals(2, iv.valueCount)
            assertEquals(IntervalDayTime(Period.ofDays(0), Duration.ofHours(1)), iv.getObject(0))
            assertEquals(IntervalDayTime(Period.ofDays(1), Duration.ofHours(0)), iv.getObject(1))
        }
    }
}