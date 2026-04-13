package xtdb.debezium

import com.google.protobuf.ByteString
import org.apache.kafka.connect.runtime.WorkerConfig
import org.apache.kafka.connect.storage.OffsetBackingStore
import org.apache.kafka.connect.util.Callback
import xtdb.debezium.proto.DebeziumOffsetEntry
import xtdb.debezium.proto.debeziumOffsetEntry
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future

// An in-memory `OffsetBackingStore` for embedded Debezium that lets us smuggle
// initial state in and read Debezium's writes back out, neither of which the
// engine's standard classname-based config supports.
//
// Debezium instantiates this class via reflection (no-arg ctor) and then calls
// `configure(WorkerConfig)`.  To bridge the instance back to our code, we pass an
// instance id through the engine's properties; `configure` looks up a pre-registered
// handle by that id so the outside can reach `snapshotEntries()`.
//
// The map lives only in memory: durability comes from the resumeToken being persisted
// atomically with the XTDB data, then fed back at the next `onPartitionAssigned`.
class DebeziumEngineOffsetStore : OffsetBackingStore {

    companion object {
        const val INSTANCE_ID_CONFIG = "xtdb.debezium.engine.offset-store.instance-id"

        // Registry of seeded state + live handles keyed by instance id.
        // The ExternalSource sets up an entry before the engine starts and removes it
        // when the engine stops.  Concurrent by construction — many sources could run
        // in the same JVM (tests, multi-db deployments).
        private val seeds = ConcurrentHashMap<String, List<DebeziumOffsetEntry>>()
        private val handles = ConcurrentHashMap<String, DebeziumEngineOffsetStore>()

        // Registers seed state for a fresh engine instance and returns a token that
        // both identifies it in Debezium config and gives the caller a way to later
        // obtain the live handle via `handleFor(id)`.
        fun register(seed: List<DebeziumOffsetEntry>): String {
            val id = UUID.randomUUID().toString()
            seeds[id] = seed
            return id
        }

        fun unregister(instanceId: String) {
            seeds.remove(instanceId)
            handles.remove(instanceId)
        }

        // The live handle, populated once Debezium has instantiated and configured the store.
        // Null before the engine starts.
        fun handleFor(instanceId: String): DebeziumEngineOffsetStore? = handles[instanceId]
    }

    private lateinit var instanceId: String

    // Thread-safe: Debezium's engine thread calls set(); our ChangeConsumer thread calls snapshotEntries().
    private val data = ConcurrentHashMap<ByteBuffer, ByteBuffer>()

    override fun configure(config: WorkerConfig) {
        val id = config.originalsStrings()[INSTANCE_ID_CONFIG]
        require(!id.isNullOrBlank()) {
            "missing or blank $INSTANCE_ID_CONFIG in WorkerConfig — DebeziumEngineOffsetStore requires a non-blank value"
        }
        instanceId = id

        seeds.remove(instanceId)?.forEach { entry ->
            data[copyOf(entry.key.asReadOnlyByteBuffer())] = copyOf(entry.value.asReadOnlyByteBuffer())
        }

        handles[instanceId] = this
    }

    override fun start() {}

    override fun stop() {
        handles.remove(instanceId, this)
    }

    override fun get(keys: Collection<ByteBuffer>): Future<Map<ByteBuffer, ByteBuffer>> =
        CompletableFuture.completedFuture(
            buildMap {
                for (k in keys) {
                    // Normalise the key and return independent views so callers can never
                    // observe or mutate the position/limit of buffers stored in `data`.
                    // Read-only protects the bytes; `duplicate()` gives each caller its
                    // own position/limit state.
                    val normalisedKey = copyOf(k)
                    data[normalisedKey]?.let { put(normalisedKey, it.duplicate()) }
                }
            }
        )

    override fun set(values: Map<ByteBuffer, ByteBuffer>, callback: Callback<Void>?): Future<Void> {
        for ((k, v) in values) {
            // Deep-copy the bytes: Debezium reuses buffers, and we hold onto them beyond this call.
            data[copyOf(k)] = copyOf(v)
        }
        callback?.onCompletion(null, null)
        return CompletableFuture.completedFuture(null)
    }

    // Required by the Kafka Connect OffsetBackingStore interface (added in newer versions)
    // for external consumers querying "what partitions does this connector know about?"
    // We don't surface that information — Debezium embedded doesn't need it, and we have
    // no external consumers of this store.
    override fun connectorPartitions(connectorName: String): MutableSet<MutableMap<String, Any>> =
        mutableSetOf()

    // Snapshot of current state for inclusion in a resumeToken.
    fun snapshotEntries(): List<DebeziumOffsetEntry> =
        data.map { (k, v) ->
            debeziumOffsetEntry {
                key = ByteString.copyFrom(k.duplicate())
                value = ByteString.copyFrom(v.duplicate())
            }
        }
}

private fun copyOf(buf: ByteBuffer): ByteBuffer {
    val bytes = ByteArray(buf.remaining())
    buf.duplicate().get(bytes)
    return ByteBuffer.wrap(bytes).asReadOnlyBuffer()
}
