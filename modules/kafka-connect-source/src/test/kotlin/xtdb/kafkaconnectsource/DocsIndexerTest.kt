package xtdb.kafkaconnectsource

import org.apache.kafka.connect.data.Date as ConnectDate
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Time as ConnectTime
import org.apache.kafka.connect.data.Timestamp as ConnectTimestamp
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.util.Date
import java.util.TimeZone

private const val MS_PER_DAY = 86_400_000L

class DocsIndexerTest {

    /** Runs [block] with the JVM default zone temporarily overridden to UTC-8 (LA). */
    private inline fun <R> withNonUtcDefaultZone(block: () -> R): R {
        val original = TimeZone.getDefault()
        TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
        try {
            return block()
        } finally {
            TimeZone.setDefault(original)
        }
    }

    @Test
    fun `Date logical type does not shift under non-UTC JVM default zone`() = withNonUtcDefaultZone {
        // Connect encodes Date as `Date(epochDay * 86_400_000)` — pinned to 00:00 UTC on the target
        // calendar day. Naive `toInstant().toLocalDate()` in a non-UTC default zone would shift
        // back a day, because midnight-UTC reads as the previous day west of Greenwich.
        val schema = SchemaBuilder.int32().name(ConnectDate.LOGICAL_NAME).build()
        val expected = LocalDate.of(2024, 6, 8)
        val connectValue = Date(expected.toEpochDay() * MS_PER_DAY)

        assertEquals(expected, convertValue(connectValue, schema))
    }

    @Test
    fun `Time logical type does not shift under non-UTC JVM default zone`() = withNonUtcDefaultZone {
        // Connect encodes Time as `Date(millis_of_day)` — pinned to 1970-01-01 UTC + the time of day.
        // Naive `toInstant().toLocalTime()` in a non-UTC default zone would subtract the offset.
        val schema = SchemaBuilder.int32().name(ConnectTime.LOGICAL_NAME).build()
        val expected = LocalTime.of(12, 34, 56)
        val millisOfDay = expected.toNanoOfDay() / 1_000_000L
        val connectValue = Date(millisOfDay)

        assertEquals(expected, convertValue(connectValue, schema))
    }

    @Test
    fun `Timestamp logical type round-trips an Instant`() {
        // Timestamps are already TZ-anchored (millis-since-epoch), so JVM zone shouldn't matter.
        // Belt-and-braces test that the conversion does what we claim.
        val schema = SchemaBuilder.int64().name(ConnectTimestamp.LOGICAL_NAME).build()
        val expected = Instant.parse("2024-06-08T12:34:56.789Z")
        val connectValue = Date.from(expected)

        assertEquals(expected, convertValue(connectValue, schema))
    }
}
