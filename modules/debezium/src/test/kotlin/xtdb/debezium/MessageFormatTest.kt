package xtdb.debezium

import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.util.Utf8
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.error.Incorrect

class MessageFormatTest {

    // -- Json format tests --

    @Test
    fun `Json decode produces map from valid CDC envelope`() {
        val json = """{"op":"c","source":{"schema":"public","table":"t"},"after":{"_id":1,"name":"Alice"}}"""
        val payload = MessageFormat.Json.decode(json.toByteArray())

        assertEquals("c", payload["op"])
        @Suppress("UNCHECKED_CAST")
        val after = payload["after"] as Map<String, Any?>
        assertEquals(1L, after["_id"])
        assertEquals("Alice", after["name"])
    }

    @Test
    fun `Json decode unwraps payload envelope`() {
        val json = """{"payload":{"op":"c","source":{"schema":"public","table":"t"},"after":{"_id":1}}}"""
        val payload = MessageFormat.Json.decode(json.toByteArray())
        assertEquals("c", payload["op"])
    }

    @Test
    fun `Json decode rejects non-ByteArray`() {
        val ex = assertThrows(Incorrect::class.java) {
            MessageFormat.Json.decode("not a byte array")
        }
        assertTrue(ex.message!!.contains("ByteArray"))
    }

    @Test
    fun `Json decode rejects invalid JSON`() {
        assertThrows(Incorrect::class.java) {
            MessageFormat.Json.decode("not json".toByteArray())
        }
    }

    // -- Avro format tests --

    private val sourceSchema = SchemaBuilder.record("Source")
        .fields()
        .requiredString("schema")
        .requiredString("table")
        .optionalLong("lsn")
        .endRecord()

    private val docSchema = SchemaBuilder.record("Doc")
        .fields()
        .requiredLong("_id")
        .requiredString("name")
        .endRecord()

    private val envelopeSchema = SchemaBuilder.record("Envelope")
        .fields()
        .requiredString("op")
        .name("source").type(sourceSchema).noDefault()
        .name("before").type().unionOf().nullType().and().type(docSchema).endUnion().nullDefault()
        .name("after").type().unionOf().nullType().and().type(docSchema).endUnion().nullDefault()
        .endRecord()

    private fun cdcRecord(
        op: String,
        schema: String = "public",
        table: String = "users",
        before: GenericData.Record? = null,
        after: GenericData.Record? = null,
    ): GenericData.Record = GenericRecordBuilder(envelopeSchema)
        .set("op", op)
        .set("source", GenericRecordBuilder(sourceSchema)
            .set("schema", schema)
            .set("table", table)
            .set("lsn", 42L)
            .build())
        .set("before", before)
        .set("after", after)
        .build()

    private fun doc(id: Long, name: String): GenericData.Record =
        GenericRecordBuilder(docSchema)
            .set("_id", id)
            .set("name", name)
            .build()

    @Test
    fun `Avro decode produces map from create op`() {
        val record = cdcRecord("c", after = doc(1L, "Alice"))
        val payload = MessageFormat.Avro.decode(record)

        assertEquals("c", payload["op"])
        @Suppress("UNCHECKED_CAST")
        val source = payload["source"] as Map<String, Any?>
        assertEquals("public", source["schema"])
        assertEquals("users", source["table"])

        @Suppress("UNCHECKED_CAST")
        val after = payload["after"] as Map<String, Any?>
        assertEquals(1L, after["_id"])
        assertEquals("Alice", after["name"])
    }

    @Test
    fun `Avro decode produces map from delete op`() {
        val record = cdcRecord("d", before = doc(3L, "to-delete"))
        val payload = MessageFormat.Avro.decode(record)

        assertEquals("d", payload["op"])
        @Suppress("UNCHECKED_CAST")
        val before = payload["before"] as Map<String, Any?>
        assertEquals(3L, before["_id"])
    }

    @Test
    fun `Avro decode converts Utf8 to String`() {
        val record = cdcRecord("c", after = doc(1L, "hello"))
        val payload = MessageFormat.Avro.decode(record)

        @Suppress("UNCHECKED_CAST")
        val after = payload["after"] as Map<String, Any?>
        assertInstanceOf(String::class.java, after["name"])
    }

    @Test
    fun `Avro decode handles arrays`() {
        val arrayDocSchema = SchemaBuilder.record("ArrayDoc")
            .fields()
            .requiredLong("_id")
            .name("tags").type().array().items().stringType().noDefault()
            .endRecord()

        val arrayEnvelopeSchema = SchemaBuilder.record("ArrayEnvelope")
            .fields()
            .requiredString("op")
            .name("source").type(sourceSchema).noDefault()
            .name("before").type().unionOf().nullType().and().type(arrayDocSchema).endUnion().nullDefault()
            .name("after").type().unionOf().nullType().and().type(arrayDocSchema).endUnion().nullDefault()
            .endRecord()

        val afterRecord = GenericData.Record(arrayDocSchema)
        afterRecord.put("_id", 1L)
        afterRecord.put("tags", GenericData.Array(arrayDocSchema.getField("tags").schema(), listOf(Utf8("a"), Utf8("b"))))

        val envelope = GenericRecordBuilder(arrayEnvelopeSchema)
            .set("op", "c")
            .set("source", GenericRecordBuilder(sourceSchema).set("schema", "public").set("table", "t").set("lsn", 1L).build())
            .set("before", null)
            .set("after", afterRecord)
            .build()

        val payload = MessageFormat.Avro.decode(envelope)
        @Suppress("UNCHECKED_CAST")
        val after = payload["after"] as Map<String, Any?>
        assertEquals(listOf("a", "b"), after["tags"])
    }

    @Test
    fun `Avro decode converts enum symbols to strings`() {
        val opEnum = SchemaBuilder.enumeration("OpEnum")
            .namespace("xtdb.debezium.test")
            .symbols("c", "r", "u", "d")

        val enumEnvelopeSchema = SchemaBuilder.record("EnumEnvelope")
            .namespace("xtdb.debezium.test")
            .fields()
            .name("op").type(opEnum).noDefault()
            .name("source").type(sourceSchema).noDefault()
            .name("before").type().unionOf().nullType().and().type(docSchema).endUnion().nullDefault()
            .name("after").type().unionOf().nullType().and().type(docSchema).endUnion().nullDefault()
            .endRecord()

        val record = GenericRecordBuilder(enumEnvelopeSchema)
            .set("op", GenericData.EnumSymbol(opEnum, "c"))
            .set("source", GenericRecordBuilder(sourceSchema)
                .set("schema", "public").set("table", "t").set("lsn", 1L).build())
            .set("before", null)
            .set("after", doc(1L, "Alice"))
            .build()

        val payload = MessageFormat.Avro.decode(record)
        assertEquals("c", payload["op"])
        assertInstanceOf(String::class.java, payload["op"])
    }

    @Test
    fun `Avro decode rejects non-GenericRecord`() {
        val ex = assertThrows(Incorrect::class.java) {
            MessageFormat.Avro.decode("not a record")
        }
        assertTrue(ex.message!!.contains("GenericRecord"))
    }

    @Test
    fun `Avro decode passes through unknown ops`() {
        val record = cdcRecord("x", after = doc(1L, "Alice"))
        val payload = MessageFormat.Avro.decode(record)
        assertEquals("x", payload["op"])
    }
}
