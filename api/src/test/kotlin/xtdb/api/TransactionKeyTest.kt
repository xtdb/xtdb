package xtdb.api

import kotlinx.serialization.encodeToString
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.JSON_SERDE
import java.time.Instant

class TransactionKeyTest {

    @Test
    fun testTransactionKeySerialization() {
        assertEquals(
            """{"txId":1,"systemTime":"1970-01-01T00:00:00Z"}""",
            JSON_SERDE.encodeToString<TransactionKey>(txKey(1, Instant.ofEpochMilli(0))).trimIndent()
        )
    }

    @Test
    fun testTransactionKeyDeserialization() {
        assertEquals(
            txKey(1L, Instant.ofEpochMilli(0)),
            JSON_SERDE.decodeFromString<TransactionKey>("""{"txId":1,"systemTime":"1970-01-01T00:00:00Z"}""")
        )
    }
}
