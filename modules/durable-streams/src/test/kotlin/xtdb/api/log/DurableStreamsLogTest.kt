package xtdb.api.log

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.Base64

class DurableStreamsLogTest {

    // ---- parseMessages unit tests -------------------------------------------

    @Test
    fun `parseMessages - empty array`() {
        assertEquals(emptyList<ByteArray>(), parseMessages("[]"))
    }

    @Test
    fun `parseMessages - blank string`() {
        assertEquals(emptyList<ByteArray>(), parseMessages("  "))
    }

    @Test
    fun `parseMessages - single element`() {
        val bytes = "hello".toByteArray()
        val b64 = Base64.getEncoder().encodeToString(bytes)
        val result = parseMessages("[\"$b64\"]")
        assertEquals(1, result.size)
        assertArrayEquals(bytes, result[0])
    }

    @Test
    fun `parseMessages - multiple elements`() {
        val msgs = listOf("hello", "world", "xtdb").map { it.toByteArray() }
        val json = "[" + msgs.joinToString(",") { "\"${Base64.getEncoder().encodeToString(it)}\"" } + "]"
        val result = parseMessages(json)
        assertEquals(3, result.size)
        msgs.forEachIndexed { i, expected ->
            assertArrayEquals(expected, result[i])
        }
    }

    @Test
    fun `parseMessages - handles whitespace around array`() {
        val bytes = "test".toByteArray()
        val b64 = Base64.getEncoder().encodeToString(bytes)
        val result = parseMessages("  [ \"$b64\" ]  ")
        assertEquals(1, result.size)
        assertArrayEquals(bytes, result[0])
    }

    @Test
    fun `parseMessages - binary data round-trips`() {
        val bytes = ByteArray(256) { it.toByte() }
        val b64 = Base64.getEncoder().encodeToString(bytes)
        val result = parseMessages("[\"$b64\"]")
        assertEquals(1, result.size)
        assertArrayEquals(bytes, result[0])
    }

    // ---- ClusterFactory builder tests ---------------------------------------

    @Test
    fun `ClusterFactory sets baseUrl`() {
        val f = DurableStreamsLog.ClusterFactory("https://example.workers.dev")
        assertEquals("https://example.workers.dev", f.baseUrl)
    }

    @Test
    fun `ClusterFactory builder methods return same instance`() {
        val f = DurableStreamsLog.ClusterFactory("https://x.example")
            .connectTimeout(java.time.Duration.ofSeconds(5))
            .longPollTimeout(java.time.Duration.ofSeconds(20))
        assertEquals(java.time.Duration.ofSeconds(5), f.connectTimeout)
        assertEquals(java.time.Duration.ofSeconds(20), f.longPollTimeout)
    }

    // ---- LogFactory builder tests -------------------------------------------

    @Test
    fun `LogFactory defaults`() {
        val f = DurableStreamsLog.LogFactory("my-cluster", "xtdb-log")
        assertEquals("my-cluster", f.cluster)
        assertEquals("xtdb-log", f.topic)
        assertEquals("xtdb-log-replica", f.replicaTopic)
        assertEquals(0, f.epoch)
        assertEquals("xtdb", f.tenant)
        assertTrue(f.createStream)
    }

    @Test
    fun `LogFactory builder methods`() {
        val f = DurableStreamsLog.LogFactory("cluster", "topic")
            .replicaTopic("replica-custom")
            .epoch(3)
            .tenant("acme")
            .createStream(false)
        assertEquals("replica-custom", f.replicaTopic)
        assertEquals(3, f.epoch)
        assertEquals("acme", f.tenant)
        assertFalse(f.createStream)
    }
}
