package xtdb.debezium

import com.google.protobuf.ByteString
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.runtime.WorkerConfig
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import xtdb.debezium.proto.DebeziumOffsetEntry
import xtdb.debezium.proto.debeziumOffsetEntry
import java.nio.ByteBuffer

class DebeziumEngineOffsetStoreTest {

    private fun configFor(id: String): WorkerConfig {
        val defs = ConfigDef()
            .define(DebeziumEngineOffsetStore.INSTANCE_ID_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, "")
        return object : WorkerConfig(defs, mapOf(DebeziumEngineOffsetStore.INSTANCE_ID_CONFIG to id)) {}
    }

    private fun entry(key: String, value: String) = debeziumOffsetEntry {
        this.key = ByteString.copyFromUtf8(key)
        this.value = ByteString.copyFromUtf8(value)
    }

    private fun buf(s: String): ByteBuffer = ByteBuffer.wrap(s.toByteArray()).asReadOnlyBuffer()

    private fun ByteBuffer.asUtf8(): String {
        val bytes = ByteArray(remaining())
        duplicate().get(bytes)
        return String(bytes, Charsets.UTF_8)
    }

    @Test
    fun `bootstrap from seed and retrieve via get`() {
        val seed = listOf(entry("k1", "v1"), entry("k2", "v2"))
        val id = DebeziumEngineOffsetStore.register(seed)

        val store = DebeziumEngineOffsetStore()
        store.configure(configFor(id))

        val retrieved = store.get(listOf(buf("k1"), buf("k2"), buf("missing"))).get()

        assertEquals(2, retrieved.size)
        assertEquals("v1", retrieved[buf("k1")]!!.asUtf8())
        assertEquals("v2", retrieved[buf("k2")]!!.asUtf8())

        store.stop()
        DebeziumEngineOffsetStore.unregister(id)
    }

    @Test
    fun `set writes persist and appear in snapshot`() {
        val id = DebeziumEngineOffsetStore.register(emptyList())
        val store = DebeziumEngineOffsetStore()
        store.configure(configFor(id))

        store.set(mapOf(buf("alpha") to buf("one"), buf("beta") to buf("two")), null).get()

        val snap = store.snapshotEntries()
        assertEquals(2, snap.size)

        val asMap = snap.associate { it.key.toStringUtf8() to it.value.toStringUtf8() }
        assertEquals("one", asMap["alpha"])
        assertEquals("two", asMap["beta"])

        store.stop()
        DebeziumEngineOffsetStore.unregister(id)
    }

    @Test
    fun `set overwrites existing entries`() {
        val id = DebeziumEngineOffsetStore.register(listOf(entry("k", "old")))
        val store = DebeziumEngineOffsetStore()
        store.configure(configFor(id))

        store.set(mapOf(buf("k") to buf("new")), null).get()

        val snap = store.snapshotEntries().associate { it.key.toStringUtf8() to it.value.toStringUtf8() }
        assertEquals("new", snap["k"])

        store.stop()
        DebeziumEngineOffsetStore.unregister(id)
    }

    @Test
    fun `handleFor returns configured store and null after stop`() {
        val id = DebeziumEngineOffsetStore.register(emptyList())
        val store = DebeziumEngineOffsetStore()
        store.configure(configFor(id))

        assertNotNull(DebeziumEngineOffsetStore.handleFor(id))

        store.stop()
        assertNull(DebeziumEngineOffsetStore.handleFor(id))

        DebeziumEngineOffsetStore.unregister(id)
    }

    @Test
    fun `multiple concurrent instances don't collide`() {
        val id1 = DebeziumEngineOffsetStore.register(listOf(entry("x", "1")))
        val id2 = DebeziumEngineOffsetStore.register(listOf(entry("x", "2")))

        val store1 = DebeziumEngineOffsetStore().also { it.configure(configFor(id1)) }
        val store2 = DebeziumEngineOffsetStore().also { it.configure(configFor(id2)) }

        assertEquals("1", store1.snapshotEntries().single().value.toStringUtf8())
        assertEquals("2", store2.snapshotEntries().single().value.toStringUtf8())

        // Writes to one don't bleed into the other.
        store1.set(mapOf(buf("y") to buf("from1")), null).get()
        assertTrue(store2.snapshotEntries().none { it.key.toStringUtf8() == "y" })

        store1.stop(); store2.stop()
        DebeziumEngineOffsetStore.unregister(id1)
        DebeziumEngineOffsetStore.unregister(id2)
    }

    @Test
    fun `get returns independent buffer views per call`() {
        // Without duplicating the stored value, advancing position on one retrieved buffer
        // would corrupt the stored buffer and poison later reads.
        val id = DebeziumEngineOffsetStore.register(listOf(entry("k", "hello")))
        val store = DebeziumEngineOffsetStore()
        store.configure(configFor(id))

        val first = store.get(listOf(buf("k"))).get()[buf("k")]!!
        first.position(first.limit())  // exhaust the first view
        assertEquals(0, first.remaining())

        val second = store.get(listOf(buf("k"))).get()[buf("k")]!!
        assertEquals(5, second.remaining())
        assertEquals("hello", second.asUtf8())

        store.stop()
        DebeziumEngineOffsetStore.unregister(id)
    }

    @Test
    fun `blank instance id in config is rejected`() {
        val store = DebeziumEngineOffsetStore()
        val thrown = runCatching { store.configure(configFor("   ")) }.exceptionOrNull()
        assertNotNull(thrown)
        assertTrue(thrown!!.message!!.contains(DebeziumEngineOffsetStore.INSTANCE_ID_CONFIG))
    }

    @Test
    fun `missing instance id in config is rejected`() {
        val store = DebeziumEngineOffsetStore()
        val emptyConfig = object : WorkerConfig(ConfigDef(), emptyMap<String, String>()) {}

        val thrown = runCatching { store.configure(emptyConfig) }.exceptionOrNull()
        assertNotNull(thrown)
        assertTrue(thrown!!.message!!.contains(DebeziumEngineOffsetStore.INSTANCE_ID_CONFIG))
    }
}
