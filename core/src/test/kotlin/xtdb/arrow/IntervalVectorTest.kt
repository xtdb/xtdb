package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.time.Interval
import kotlin.time.Duration.Companion.hours

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
        IntervalMonthDayNanoVector(allocator, "imdnv", false).use { iv ->
            iv.writeObject(Interval(0, 0, 1.hours.inWholeNanoseconds))
            iv.writeObject(Interval(1, 0, 0))

            assertEquals(2, iv.valueCount)
            assertEquals(Interval(0, 0, 1.hours.inWholeNanoseconds), iv.getObject(0))
            assertEquals(Interval(1, 0, 0), iv.getObject(1))
        }
    }

    @Test
    fun testIntervalMonthDayMicroReadWrite() {
        IntervalMonthDayMicroVector(IntervalMonthDayNanoVector(allocator, "imdmv", false)).use { iv ->
            iv.writeObject(Interval(0, 0, 1.hours.inWholeNanoseconds))
            iv.writeObject(Interval(1, 0, 0))

            assertEquals(2, iv.valueCount)
            assertEquals(Interval(0, 0, 1.hours.inWholeNanoseconds), iv.getObject(0))
            assertEquals(Interval(1, 0, 0), iv.getObject(1))
        }
    }

    @Test
    fun testIntervalDayTimeReadWrite() {
        IntervalDayTimeVector(allocator, "idtv", false).use { iv ->
            iv.writeObject(Interval(0, 0, 1.hours.inWholeNanoseconds))
            iv.writeObject(Interval(0, 1, 0))

            assertEquals(2, iv.valueCount)
            assertEquals(Interval(0, 0, 1.hours.inWholeNanoseconds), iv.getObject(0))
            assertEquals(Interval(0, 1, 0), iv.getObject(1))
        }
    }
}