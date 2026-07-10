package xtdb.kafka.connectsrc

import clojure.lang.Keyword
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Timestamp as ConnectTimestamp
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.error.Anomaly
import xtdb.error.Incorrect
import xtdb.table.TableRef
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

    private fun recWithKey(key: Any?, keySchema: Schema? = null) =
        SinkRecord("t", 0, keySchema, key, null, mapOf("name" to "x"), 42)

    private val Anomaly.errorCode: Keyword?
        get() = data.valAt(Keyword.intern("xtdb.error", "code")) as? Keyword

    @Test
    fun `table field parses through TableRef`() {
        fun tableOf(table: String) = (DocsIndexer.Factory(table = table).open() as DocsIndexer).table

        assertEquals(TableRef("public", "events"), tableOf("events"))
        assertEquals(TableRef("analytics", "events"), tableOf("analytics.events"))
    }

    @Test
    fun `resolveId passes a String key through`() {
        assertEquals("k1", resolveId(recWithKey("k1")))
    }

    @Test
    fun `resolveId unwraps a single-field Struct key`() {
        val keySchema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build()
        val key = Struct(keySchema).put("id", "k1")

        assertEquals("k1", resolveId(recWithKey(key, keySchema)))
    }

    @Test
    fun `keyless record is rejected with docs-no-key`() {
        val e = assertThrows<Incorrect> { resolveId(recWithKey(null)) }

        assertEquals(Keyword.intern("xtdb.kafka-connect-source", "docs-no-key"), e.errorCode)
    }

    @Test
    fun `multi-field Struct key is rejected with docs-invalid-key`() {
        val keySchema = SchemaBuilder.struct()
            .field("region", Schema.STRING_SCHEMA)
            .field("id", Schema.STRING_SCHEMA)
            .build()
        val key = Struct(keySchema).put("region", "eu").put("id", "k1")

        val e = assertThrows<Incorrect> { resolveId(recWithKey(key, keySchema)) }

        assertEquals(Keyword.intern("xtdb.kafka-connect-source", "docs-invalid-key"), e.errorCode)
        assertTrue(e.message!!.contains("ExtractField"), "error should point at the SMT remedy, got: ${e.message}")
    }

    @Test
    fun `byte array key is rejected with docs-invalid-key`() {
        val e = assertThrows<Incorrect> { resolveId(recWithKey("k1".toByteArray(), Schema.BYTES_SCHEMA)) }

        assertEquals(Keyword.intern("xtdb.kafka-connect-source", "docs-invalid-key"), e.errorCode)
    }

    @Test
    fun `single-field Struct key with a null value reports an unusable key`() {
        val keySchema = SchemaBuilder.struct().field("id", Schema.OPTIONAL_STRING_SCHEMA).build()
        val key = Struct(keySchema)

        val e = assertThrows<Incorrect> { resolveId(recWithKey(key, keySchema)) }

        assertEquals(Keyword.intern("xtdb.kafka-connect-source", "docs-no-key"), e.errorCode)
        assertTrue(e.message!!.contains("no usable key"), "message shouldn't claim the key is absent, got: ${e.message}")
    }
}
