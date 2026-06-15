package xtdb.kafka.connectsrc

import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Timestamp as ConnectTimestamp
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.Date

class DocsIndexerTest {

    @Test
    fun `Timestamp logical type round-trips an Instant`() {
        val schema = SchemaBuilder.int64().name(ConnectTimestamp.LOGICAL_NAME).build()
        val expected = Instant.parse("2024-06-08T12:34:56.789Z")
        val connectValue = Date.from(expected)

        assertEquals(expected, convertValue(connectValue, schema))
    }
}
