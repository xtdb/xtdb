package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.types.Interval
import xtdb.types.Interval.*
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
            iv.writeObject(MonthDayNano(0, 0, 1.hours.inWholeNanoseconds))
            iv.writeObject(MonthDayNano(1, 0, 0))

            assertEquals(2, iv.valueCount)
            assertEquals(MonthDayNano(0, 0, 1.hours.inWholeNanoseconds), iv.getObject(0))
            assertEquals(MonthDayNano(1, 0, 0), iv.getObject(1))
        }
    }

    @Test
    fun testIntervalMonthDayMicroReadWrite() {
        IntervalMonthDayMicroVector(IntervalMonthDayNanoVector(allocator, "imdmv", false)).use { iv ->
            iv.writeObject(MonthDayMicro(0, 0, 1.hours.inWholeNanoseconds))
            iv.writeObject(MonthDayMicro(1, 0, 0))

            assertEquals(2, iv.valueCount)
            assertEquals(MonthDayMicro(0, 0, 1.hours.inWholeNanoseconds), iv.getObject(0))
            assertEquals(MonthDayMicro(1, 0, 0), iv.getObject(1))
        }
    }

    @Test
    fun testIntervalDayTimeReadWrite() {
        IntervalDayTimeVector(allocator, "idtv", false).use { iv ->
            iv.writeObject(DayTime(0, 1.hours.inWholeMilliseconds.toInt()))
            iv.writeObject(DayTime(1, 0))

            assertEquals(2, iv.valueCount)
            assertEquals(DayTime(0, 1.hours.inWholeMilliseconds.toInt()), iv.getObject(0))
            assertEquals(DayTime(1, 0), iv.getObject(1))
        }
    }
}