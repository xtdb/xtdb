package xtdb.index

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.TimeUnit
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.DateDayVector
import xtdb.arrow.DateMilliVector
import xtdb.arrow.TimestampLocalVector
import xtdb.arrow.TimestampTzVector
import xtdb.arrow.Vector
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset.UTC

class IndexHashTest {
    private lateinit var al: BufferAllocator

    private val london = ZoneId.of("Europe/London")

    @BeforeEach
    fun setUp() {
        al = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        al.close()
    }

    private fun Vector.hashOf(value: Any) = use { it.writeObject(value); IndexHash.of(it, 0) }

    private fun localHash(unit: TimeUnit, value: LocalDateTime) = TimestampLocalVector(al, "v", false, unit).hashOf(value)
    private fun instantHash(unit: TimeUnit, value: Instant, zone: ZoneId) = TimestampTzVector(al, "v", false, unit, zone).hashOf(value)

    @Test
    fun `a microsecond timestamp hashes differently once held to the nanosecond`() {
        val local = LocalDateTime.of(2021, 6, 1, 12, 34, 56, 123_458_000)
        assertNotEquals(localHash(TimeUnit.MICROSECOND, local), localHash(TimeUnit.NANOSECOND, local))
    }

    @Test
    fun `a local timestamp's hashes cover it at every precision that holds it, and as a date at midnight`() {
        val fractional = LocalDateTime.of(2021, 6, 1, 12, 34, 56, 123_458_000)
        val fractionalHashes = IndexHash.ofLocal(al, fractional, london)
        for (unit in listOf(TimeUnit.MICROSECOND, TimeUnit.NANOSECOND))
            assertTrue(localHash(unit, fractional) in fractionalHashes, "$unit")

        val midnight = LocalDateTime.of(2021, 10, 31, 0, 0)
        val midnightHashes = IndexHash.ofLocal(al, midnight, london)
        for (unit in TimeUnit.entries)
            assertTrue(localHash(unit, midnight) in midnightHashes, "$unit")
        assertTrue(DateDayVector(al, "v", false).hashOf(midnight.toLocalDate()) in midnightHashes)
        assertTrue(DateMilliVector(al, "v", false).hashOf(midnight.toLocalDate()) in midnightHashes)
    }

    @Test
    fun `a wall-clock reading's hashes cover every instant the session zone reads as it`() {
        val overlap = IndexHash.ofLocal(al, LocalDateTime.of(2021, 10, 31, 1, 30), london)

        for (instant in listOf(Instant.parse("2021-10-31T00:30:00Z"), Instant.parse("2021-10-31T01:30:00Z")))
            for (unit in TimeUnit.entries)
                for (zone in listOf(UTC, london))
                    assertTrue(instantHash(unit, instant, zone) in overlap, "$instant $unit $zone")
    }

    @Test
    fun `an instant's hashes cover it in any zone at every precision, and its wall-clock reading in the session zone`() {
        val instant = Instant.parse("2021-06-01T11:34:56.123458Z")
        val hashes = IndexHash.ofInstant(al, instant, london)

        for (unit in listOf(TimeUnit.MICROSECOND, TimeUnit.NANOSECOND)) {
            assertTrue(instantHash(unit, instant, UTC) in hashes, "$unit")
            assertTrue(instantHash(unit, instant, london) in hashes, "$unit")
            assertTrue(localHash(unit, LocalDateTime.of(2021, 6, 1, 12, 34, 56, 123_458_000)) in hashes, "$unit")
        }

        val londonMidnight = IndexHash.ofInstant(al, Instant.parse("2021-10-30T23:00:00Z"), london)
        assertTrue(DateDayVector(al, "v", false).hashOf(LocalDate.of(2021, 10, 31)) in londonMidnight)
    }
}
