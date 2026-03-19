package xtdb.util

import org.apache.arrow.vector.types.pojo.Schema
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.nio.ByteBuffer

class ArrowUtilTest {

    private val schema = Schema(emptyList())

    @Test
    fun `serializeAsMessageInterruptibly - round trip`() {
        val bytes = schema.serializeAsMessageInterruptibly()
        val deserialized = ByteBuffer.wrap(bytes).deserializeMessageAsSchemaInterruptibly()
        assertEquals(schema, deserialized)
    }

    @Test
    fun `serializeAsMessageInterruptibly - throws InterruptedException when thread is interrupted`() {
        Thread.currentThread().interrupt()
        try {
            assertThrows<InterruptedException> {
                schema.serializeAsMessageInterruptibly()
            }
        } finally {
            Thread.interrupted() // clear flag in case the exception wasn't thrown
        }
    }
}