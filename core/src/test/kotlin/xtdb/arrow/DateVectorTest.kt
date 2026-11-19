package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorType.Companion.DATE_DAY
import xtdb.arrow.VectorType.Scalar
import java.time.LocalDate

class DateVectorTest {
    private lateinit var allocator: BufferAllocator

    private val dateMilli = Scalar(ArrowType.Date(DateUnit.MILLISECOND))

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    @Test
    fun `a date-milli vector's getLong is its stored epoch millis`() {
        val dates = listOf(LocalDate.of(2023, 5, 17), LocalDate.of(1969, 12, 31))

        Vector.fromList(allocator, "v", dateMilli, dates).use { vec ->
            assertEquals(1_684_281_600_000L, vec.getLong(0))
            assertEquals(-86_400_000L, vec.getLong(1))
            assertEquals(dates, vec.asList)
        }
    }

    @Test
    fun `a date-milli vector reads a part-day millis value as the date it falls on`() {
        allocator.openVector("v", dateMilli).use { vec ->
            vec.writeLong(-1)
            vec.writeLong(86_400_000L + 1)

            assertEquals(listOf(LocalDate.of(1969, 12, 31), LocalDate.of(1970, 1, 2)), vec.asList)
        }
    }

    @Test
    fun `date-day and date-milli vectors agree on the metadata double of a date`() {
        val dates = listOf(LocalDate.of(2023, 5, 17), LocalDate.of(1969, 12, 31))

        Vector.fromList(allocator, "day", DATE_DAY, dates).use { dayVec ->
            Vector.fromList(allocator, "milli", dateMilli, dates).use { milliVec ->
                dayVec as DateDayVector
                milliVec as DateMilliVector
                for (idx in dates.indices) assertEquals(dayVec.getMetaDouble(idx), milliVec.getMetaDouble(idx))
            }
        }
    }
}
