package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.error.Incorrect
import java.time.Duration
import java.time.LocalDate

class DateVectorTest {
    private lateinit var al: BufferAllocator

    @BeforeEach
    fun setUp() {
        al = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        al.close()
    }

    @Test
    fun `date-day vector round-trips its extremes and rejects a date beyond them`() {
        val min = LocalDate.ofEpochDay(Int.MIN_VALUE.toLong())
        val max = LocalDate.ofEpochDay(Int.MAX_VALUE.toLong())

        DateDayVector(al, "d", false).use { vec ->
            vec.writeObject(min)
            vec.writeObject(max)
            assertEquals(listOf(min, max), vec.asList)

            assertThrows<Incorrect> { vec.writeObject(min.minusDays(1)) }
            assertThrows<Incorrect> { vec.writeObject(max.plusDays(1)) }
            assertThrows<Incorrect> { vec.writeObject(LocalDate.MAX) }
            assertEquals(2, vec.valueCount)
        }
    }

    @Test
    fun `date-milli vector round-trips its extremes and rejects a date beyond them`() {
        val millisPerDay = Duration.ofDays(1).toMillis()
        val min = LocalDate.ofEpochDay(Long.MIN_VALUE / millisPerDay)
        val max = LocalDate.ofEpochDay(Long.MAX_VALUE / millisPerDay)

        DateMilliVector(al, "d", false).use { vec ->
            vec.writeObject(min)
            vec.writeObject(max)
            assertEquals(listOf(min, max), vec.asList)

            assertThrows<Incorrect> { vec.writeObject(min.minusDays(1)) }
            assertThrows<Incorrect> { vec.writeObject(max.plusDays(1)) }
            assertThrows<Incorrect> { vec.writeObject(LocalDate.MIN) }
            assertEquals(2, vec.valueCount)
        }
    }
}
