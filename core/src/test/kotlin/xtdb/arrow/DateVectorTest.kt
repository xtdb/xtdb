package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.error.Incorrect
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorType.Companion.DATE_DAY
import xtdb.arrow.VectorType.Scalar
import java.time.Duration
import java.time.LocalDate

class DateVectorTest {
    private lateinit var al: BufferAllocator

    private val dateMilli = Scalar(ArrowType.Date(DateUnit.MILLISECOND))

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

        DateDayVector(al, false).use { vec ->
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

        DateMilliVector(al, false).use { vec ->
            vec.writeObject(min)
            vec.writeObject(max)
            assertEquals(listOf(min, max), vec.asList)

            assertThrows<Incorrect> { vec.writeObject(min.minusDays(1)) }
            assertThrows<Incorrect> { vec.writeObject(max.plusDays(1)) }
            assertThrows<Incorrect> { vec.writeObject(LocalDate.MIN) }
            assertEquals(2, vec.valueCount)
        }
    }

    @Test
    fun `a date-milli vector's getLong is its stored epoch millis`() {
        val dates = listOf(LocalDate.of(2023, 5, 17), LocalDate.of(1969, 12, 31))

        Vector.fromList(al, dateMilli, dates).use { vec ->
            assertEquals(1_684_281_600_000L, vec.getLong(0))
            assertEquals(-86_400_000L, vec.getLong(1))
            assertEquals(dates, vec.asList)
        }
    }

    @Test
    fun `a date-milli vector reads a part-day millis value as the date it falls on`() {
        al.openVector(dateMilli).use { vec ->
            vec.writeLong(-1)
            vec.writeLong(86_400_000L + 1)

            assertEquals(listOf(LocalDate.of(1969, 12, 31), LocalDate.of(1970, 1, 2)), vec.asList)
        }
    }

    @Test
    fun `date-day and date-milli vectors agree on the metadata double of a date`() {
        val dates = listOf(LocalDate.of(2023, 5, 17), LocalDate.of(1969, 12, 31))

        Vector.fromList(al, DATE_DAY, dates).use { dayVec ->
            Vector.fromList(al, dateMilli, dates).use { milliVec ->
                dayVec as DateDayVector
                milliVec as DateMilliVector
                for (idx in dates.indices) assertEquals(dayVec.getMetaDouble(idx), milliVec.getMetaDouble(idx))
            }
        }
    }
}
