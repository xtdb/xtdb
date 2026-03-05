package xtdb.debezium

import kotlinx.serialization.json.*
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import xtdb.error.Incorrect
import xtdb.tx.TxOp
import java.time.Instant

class CdcEventTest {

    private lateinit var allocator: RootAllocator

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    private fun cdcEnvelope(
        op: String,
        before: JsonObject? = null,
        after: JsonObject? = null,
        schema: String = "public",
        table: String = "test_items",
        tsNs: Long = 1700000000000000000L,
        lsn: Long = 12345L,
        wrapInPayload: Boolean = true,
    ): JsonObject {
        val cdcPayload = buildJsonObject {
            put("op", op)
            if (before != null) put("before", before) else put("before", JsonNull)
            if (after != null) put("after", after) else put("after", JsonNull)
            putJsonObject("source") {
                put("schema", schema)
                put("table", table)
                put("ts_ns", tsNs)
                put("lsn", lsn)
            }
        }

        return if (wrapInPayload) buildJsonObject { put("payload", cdcPayload) } else cdcPayload
    }

    // -- JSON conversion tests --

    @Test
    fun `toJvmMap converts primitives correctly`() {
        val obj = buildJsonObject {
            put("str", "hello")
            put("int", 42)
            put("long", 9999999999L)
            put("double", 3.14)
            put("bool", true)
            put("nil", JsonNull)
        }
        val map = obj.toJvmMap()

        assertEquals("hello", map["str"])
        assertEquals(42L, map["int"])
        assertEquals(9999999999L, map["long"])
        assertEquals(3.14, map["double"])
        assertEquals(true, map["bool"])
        assertNull(map["nil"])
    }

    @Test
    fun `toJvmMap converts nested structures`() {
        val obj = buildJsonObject {
            putJsonArray("tags") { add("a"); add("b"); add(1) }
            putJsonObject("address") {
                put("city", "London")
                put("zip", 12345)
            }
            putJsonArray("matrix") {
                addJsonArray { add(1); add(2) }
                addJsonArray { add(3); add(4) }
            }
        }
        val map = obj.toJvmMap()

        assertEquals(listOf("a", "b", 1L), map["tags"])
        assertEquals(mapOf("city" to "London", "zip" to 12345L), map["address"])
        assertEquals(listOf(listOf(1L, 2L), listOf(3L, 4L)), map["matrix"])
    }

    // -- CdcEvent parsing tests --

    @ParameterizedTest
    @ValueSource(strings = ["c", "r", "u"])
    fun `create, read and update ops produce Put event`(op: String) {
        val after = buildJsonObject { put("_id", 1); put("name", "Alice") }
        val event = CdcEvent.fromJson(cdcEnvelope(op, after = after))

        assertInstanceOf(CdcEvent.Put::class.java, event)
        event as CdcEvent.Put
        assertEquals("public", event.schema)
        assertEquals("test_items", event.table)
        assertEquals(12345L, event.lsn)
        assertEquals(1L, event.doc["_id"])
        assertEquals("Alice", event.doc["name"])
        assertNull(event.validFrom)
        assertNull(event.validTo)
    }

    @Test
    fun `delete op produces Delete event`() {
        val before = buildJsonObject { put("_id", 3); put("name", "to-delete") }
        val event = CdcEvent.fromJson(cdcEnvelope("d", before = before))

        assertInstanceOf(CdcEvent.Delete::class.java, event)
        event as CdcEvent.Delete
        assertEquals("test_items", event.table)
        assertEquals(3L, event.id)
    }

    @Test
    fun `unknown op throws`() {
        val after = buildJsonObject { put("_id", 1) }
        val envelope = cdcEnvelope("x", after = after)

        val ex = assertThrows(Incorrect::class.java) {
            CdcEvent.fromJson(envelope)
        }
        assertTrue(ex.message!!.contains("Unknown CDC op"))
    }

    @Test
    fun `missing _id in after throws`() {
        val after = buildJsonObject { put("name", "no-id") }
        val envelope = cdcEnvelope("c", after = after)

        val ex = assertThrows(Incorrect::class.java) {
            CdcEvent.fromJson(envelope)
        }
        assertTrue(ex.message!!.contains("_id"))
    }

    @Test
    fun `missing source schema throws`() {
        val envelope = buildJsonObject {
            putJsonObject("payload") {
                put("op", "c")
                putJsonObject("after") { put("_id", 1) }
                putJsonObject("source") { put("table", "t") }
            }
        }
        val ex = assertThrows(Incorrect::class.java) {
            CdcEvent.fromJson(envelope)
        }
        assertTrue(ex.message!!.contains("source.schema"))
    }

    @Test
    fun `missing source table throws`() {
        val envelope = buildJsonObject {
            putJsonObject("payload") {
                put("op", "c")
                putJsonObject("after") { put("_id", 1) }
                putJsonObject("source") { put("schema", "public") }
            }
        }
        val ex = assertThrows(Incorrect::class.java) {
            CdcEvent.fromJson(envelope)
        }
        assertTrue(ex.message!!.contains("source.table"))
    }

    @Test
    fun `delete with null before throws`() {
        val envelope = cdcEnvelope("d")

        val ex = assertThrows(Incorrect::class.java) {
            CdcEvent.fromJson(envelope)
        }
        assertTrue(ex.message!!.contains("before"))
    }

    @Test
    fun `_valid_from absent defaults to null`() {
        val after = buildJsonObject { put("_id", 1); put("name", "no-vf") }
        val event = CdcEvent.fromJson(cdcEnvelope("c", after = after)) as CdcEvent.Put
        assertNull(event.validFrom)
    }

    @Test
    fun `_valid_from and _valid_to extracted from ISO strings`() {
        val after = buildJsonObject {
            put("_id", 1); put("name", "bounded")
            put("_valid_from", "2024-01-01T00:00:00Z")
            put("_valid_to", "2025-01-01T00:00:00Z")
        }
        val event = CdcEvent.fromJson(cdcEnvelope("c", after = after)) as CdcEvent.Put
        assertEquals(Instant.parse("2024-01-01T00:00:00Z"), event.validFrom)
        assertEquals(Instant.parse("2025-01-01T00:00:00Z"), event.validTo)
    }

    @Test
    fun `_valid_to without _valid_from throws`() {
        val after = buildJsonObject {
            put("_id", 1); put("name", "end-only")
            put("_valid_to", "2025-01-01T00:00:00Z")
        }
        val ex = assertThrows(Incorrect::class.java) {
            CdcEvent.fromJson(cdcEnvelope("c", after = after))
        }
        assertTrue(ex.message!!.contains("_valid_from"))
    }

    @Test
    fun `non-string _valid_from throws`() {
        val after = buildJsonObject {
            put("_id", 1); put("_valid_from", 1704067200000000L)
        }
        val ex = assertThrows(Incorrect::class.java) {
            CdcEvent.fromJson(cdcEnvelope("c", after = after))
        }
        assertTrue(ex.message!!.contains("TIMESTAMPTZ"))
    }

    @Test
    fun `malformed _valid_from string throws`() {
        val after = buildJsonObject {
            put("_id", 1); put("_valid_from", "not-a-date")
        }
        val ex = assertThrows(Incorrect::class.java) {
            CdcEvent.fromJson(cdcEnvelope("c", after = after))
        }
        assertTrue(ex.message!!.contains("ISO-8601"))
    }

    @Test
    fun `schema and table come from source`() {
        val after = buildJsonObject { put("_id", 1) }
        val event = CdcEvent.fromJson(cdcEnvelope("c", after = after, schema = "myschema", table = "users"))

        event as CdcEvent.Put
        assertEquals("myschema", event.schema)
        assertEquals("users", event.table)
    }

    // -- Non-enveloped (schemas.enable=false) tests --

    @Test
    fun `non-enveloped create produces Put event`() {
        val after = buildJsonObject { put("_id", 1); put("name", "Alice") }
        val event = CdcEvent.fromJson(cdcEnvelope("c", after = after, wrapInPayload = false))

        assertInstanceOf(CdcEvent.Put::class.java, event)
        event as CdcEvent.Put
        assertEquals("public", event.schema)
        assertEquals("test_items", event.table)
        assertEquals(1L, event.doc["_id"])
        assertEquals("Alice", event.doc["name"])
    }

    @Test
    fun `non-enveloped delete produces Delete event`() {
        val before = buildJsonObject { put("_id", 3); put("name", "to-delete") }
        val event = CdcEvent.fromJson(cdcEnvelope("d", before = before, wrapInPayload = false))

        assertInstanceOf(CdcEvent.Delete::class.java, event)
        event as CdcEvent.Delete
        assertEquals(3L, event.id)
    }

    // -- TxOp conversion tests --

    @Test
    fun `Put toTxOp produces PutDocs`() {
        val after = buildJsonObject { put("_id", 1); put("name", "Alice") }
        val event = CdcEvent.fromJson(cdcEnvelope("c", after = after)) as CdcEvent.Put

        event.toTxOp(allocator).use { op ->
            assertInstanceOf(TxOp.PutDocs::class.java, op)
            op as TxOp.PutDocs
            assertEquals("public", op.schema)
            assertEquals("test_items", op.table)
            assertNull(op.validFrom)
            assertNull(op.validTo)
            assertEquals(1, op.docs.rowCount)
        }
    }

    @Test
    fun `Delete toTxOp produces DeleteDocs`() {
        val before = buildJsonObject { put("_id", 3); put("name", "to-delete") }
        val event = CdcEvent.fromJson(cdcEnvelope("d", before = before)) as CdcEvent.Delete

        event.toTxOp(allocator).use { op ->
            assertInstanceOf(TxOp.DeleteDocs::class.java, op)
            op as TxOp.DeleteDocs
            assertEquals("test_items", op.table)
            assertEquals(1, op.ids.valueCount)
        }
    }

    @Test
    fun `Put with validFrom produces PutDocs with validFrom`() {
        val after = buildJsonObject {
            put("_id", 1); put("name", "timed"); put("_valid_from", "2024-01-01T00:00:00Z")
        }
        val event = CdcEvent.fromJson(cdcEnvelope("c", after = after)) as CdcEvent.Put

        event.toTxOp(allocator).use { op ->
            op as TxOp.PutDocs
            assertEquals(Instant.parse("2024-01-01T00:00:00Z"), op.validFrom)
        }
    }
}
