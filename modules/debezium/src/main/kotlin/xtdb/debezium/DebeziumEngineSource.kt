package xtdb.debezium

import io.debezium.connector.postgresql.PostgresConnector
import io.debezium.engine.ChangeEvent
import io.debezium.engine.DebeziumEngine
import io.debezium.engine.format.Json
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import org.apache.arrow.memory.RootAllocator
import org.slf4j.LoggerFactory
import xtdb.api.TransactionKey
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.database.ExternalSource
import xtdb.database.ExternalSourceToken
import xtdb.database.proto.DatabaseConfig
import xtdb.debezium.proto.DebeziumEngineOffsetToken
import xtdb.debezium.proto.DebeziumEngineSourceConfig
import xtdb.debezium.proto.DebeziumEngineSourceConfig.BackendCase
import xtdb.debezium.proto.debeziumEngineOffsetToken
import xtdb.debezium.proto.debeziumEngineSourceConfig
import xtdb.debezium.proto.postgresBackendConfig
import xtdb.indexer.Indexer.Companion.addTxRow
import xtdb.indexer.OpenTx
import xtdb.time.InstantUtil
import java.time.Instant
import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import com.google.protobuf.Any as ProtoAny

private val LOG = LoggerFactory.getLogger(DebeziumEngineSource::class.java)

// Consumes CDC events via an embedded Debezium engine — reads directly from the source
// DB, no Kafka in between.  Connector-specific bits (Postgres, MySQL, …) are held in
// `Factory.backend` as a polymorphic value.
//
// One source transaction maps to one XTDB transaction.  Boundary detection is by
// `source.txId` change on streaming events.  Snapshot events are batched into
// fixed-size chunks since they share a position and aren't naturally grouped.
//
// Exactly-once-at-sink: the `resumeToken` is committed atomically with each XTDB tx.
// On restart we dedup replays by comparing the event's source position against
// `last_applied_lsn`.
class DebeziumEngineSource(
    private val dbName: String,
    private val cfg: Factory,
) : ExternalSource {

    private val allocator = RootAllocator()

    /**
     * Config for a Debezium source consuming CDC via an embedded engine.
     * The connector-specific details live in `backend` — currently `!Postgres` only.
     */
    @Serializable
    @SerialName("!DebeziumEngine")
    data class Factory(
        val backend: Backend,
        val snapshotChunkSize: Int = 1000,
        val offsetFlushIntervalMs: Long = 1000L,
    ) : ExternalSource.Factory {

        override fun open(dbName: String, clusters: Map<LogClusterAlias, Log.Cluster>): ExternalSource =
            // Embedded engine talks to the source DB directly; no `Log.Cluster` involvement.
            DebeziumEngineSource(dbName, this)

        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            dbConfig.externalSource = ProtoAny.pack(debeziumEngineSourceConfig {
                snapshotChunkSize = this@Factory.snapshotChunkSize
                offsetFlushIntervalMs = this@Factory.offsetFlushIntervalMs
                when (val b = backend) {
                    is Backend.Postgres -> postgres = postgresBackendConfig {
                        hostname = b.hostname
                        user = b.user
                        password = b.password
                        database = b.database
                        publicationName = b.publicationName
                        slotName = b.slotName
                        tableIncludeList = b.tableIncludeList
                        port = b.port
                    }
                }
            }, "proto.xtdb.com")
        }

        class Registration : ExternalSource.Registration {
            override val protoTag: String
                get() = "proto.xtdb.com/xtdb.debezium.proto.DebeziumEngineSourceConfig"

            override fun fromProto(msg: ProtoAny): ExternalSource.Factory {
                val config = msg.unpack(DebeziumEngineSourceConfig::class.java)
                val backend = when (config.backendCase) {
                    BackendCase.POSTGRES -> Backend.Postgres(
                        hostname = config.postgres.hostname,
                        user = config.postgres.user,
                        password = config.postgres.password,
                        database = config.postgres.database,
                        publicationName = config.postgres.publicationName,
                        slotName = config.postgres.slotName,
                        tableIncludeList = config.postgres.tableIncludeList,
                        port = config.postgres.port,
                    )

                    BackendCase.BACKEND_NOT_SET, null ->
                        error("DebeziumEngineSourceConfig missing required `backend`")
                }
                return Factory(
                    backend = backend,
                    snapshotChunkSize = config.snapshotChunkSize,
                    offsetFlushIntervalMs = config.offsetFlushIntervalMs,
                )
            }

            override fun registerSerde(builder: PolymorphicModuleBuilder<ExternalSource.Factory>) {
                builder.subclass(Factory::class)
            }
        }
    }

    /**
     * Connector-specific config — one variant per supported Debezium connector.
     * Responsible for knowing how to stamp its own properties onto the engine's `Properties`.
     */
    @Serializable
    sealed interface Backend {
        fun applyEngineProperties(props: Properties)

        @Serializable
        @SerialName("!Postgres")
        data class Postgres(
            val hostname: String,
            val user: String,
            val password: String,
            val database: String,
            // Publication is created on the source DB first (it defines which tables emit CDC);
            // the replication slot is our consumer's handle, created by Debezium against it.
            val publicationName: String,
            val slotName: String,
            val tableIncludeList: String,
            val port: Int = 5432,
        ) : Backend {
            override fun applyEngineProperties(props: Properties) {
                props.setProperty("connector.class", PostgresConnector::class.java.name)
                props.setProperty("database.hostname", hostname)
                props.setProperty("database.port", port.toString())
                props.setProperty("database.user", user)
                props.setProperty("database.password", password)
                props.setProperty("database.dbname", database)
                // Namespace identifier — feeds the `server` key in the OffsetBackingStore
                // partition and the `topic` field on records (even though nothing's produced
                // to Kafka in embedded mode).
                props.setProperty("topic.prefix", "xtdb-$database")

                props.setProperty("plugin.name", "pgoutput")
                props.setProperty("slot.name", slotName)
                props.setProperty("publication.name", publicationName)
                props.setProperty("publication.autocreate.mode", "disabled")
                props.setProperty("table.include.list", tableIncludeList)
            }
        }
    }

    override suspend fun onPartitionAssigned(
        partition: Int,
        afterToken: ExternalSourceToken?,
        txHandler: ExternalSource.TxHandler,
    ) = coroutineScope {
        val token = afterToken?.unpack(DebeziumEngineOffsetToken::class.java)
        val offsetStoreId = DebeziumEngineOffsetStore.register(token?.debeziumOffsetsList.orEmpty())
        try {
            val completion = CompletableDeferred<Throwable?>()

            val engine = DebeziumEngine.create(Json::class.java)
                .using(buildEngineProperties(cfg, offsetStoreId))
                .using(DebeziumEngine.CompletionCallback { success, msg, err ->
                    completion.complete(
                        when {
                            success -> null
                            err != null -> err
                            else -> RuntimeException(msg ?: "Debezium engine failed with no error")
                        }
                    )
                })
                .notifying(ChangeConsumer(dbName, allocator, offsetStoreId, txHandler, token, cfg.snapshotChunkSize))
                .build()

            val executor = Executors.newSingleThreadExecutor { r ->
                Thread(r, "xtdb-debezium-engine-$dbName").apply { isDaemon = true }
            }

            try {
                executor.execute(engine)
                val err = completion.await()
                if (err != null) throw err
            } finally {
                withContext(NonCancellable) {
                    runCatching { engine.close() }
                    executor.shutdown()
                    if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                        LOG.warn("Debezium engine didn't terminate in 30s; forcing shutdown")
                        executor.shutdownNow()
                    }
                }
            }
        } finally {
            DebeziumEngineOffsetStore.unregister(offsetStoreId)
        }
    }

    override fun close() {
        allocator.close()
    }
}

// Engine properties: engine-level config here, connector-level config delegated to the backend.
// No Kafka-broker settings because nothing is produced to Kafka in embedded mode.
private fun buildEngineProperties(cfg: DebeziumEngineSource.Factory, offsetStoreId: String): Properties =
    Properties().apply {
        setProperty("name", "xtdb-debezium-engine-${offsetStoreId.take(8)}")

        // In-memory offset store; the store instance is bridged through a static registry
        // keyed by `offsetStoreId` because Debezium instantiates OffsetBackingStore via reflection.
        setProperty("offset.storage", DebeziumEngineOffsetStore::class.java.name)
        setProperty(DebeziumEngineOffsetStore.INSTANCE_ID_CONFIG, offsetStoreId)
        setProperty("offset.flush.interval.ms", cfg.offsetFlushIntervalMs.toString())

        // Snapshot once, stream forever, fail loudly on slot/binlog loss.
        setProperty("snapshot.mode", "initial")

        // Records delivered to our callback as JSON strings; payload only, no envelope.
        setProperty("key.converter", "org.apache.kafka.connect.json.JsonConverter")
        setProperty("value.converter", "org.apache.kafka.connect.json.JsonConverter")
        setProperty("key.converter.schemas.enable", "false")
        setProperty("value.converter.schemas.enable", "false")

        cfg.backend.applyEngineProperties(this)
    }

// Engine callback.  Runs on the engine's own thread; blocks synchronously while we
// commit to XTDB (via `runBlocking`) so Debezium doesn't poll the next batch until
// this one is durably applied.
private class ChangeConsumer(
    private val dbName: String,
    private val allocator: RootAllocator,
    private val offsetStoreId: String,
    private val txHandler: ExternalSource.TxHandler,
    initialToken: DebeziumEngineOffsetToken?,
    private val snapshotChunkSize: Int,
) : DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>> {

    // Dedup high-water mark: any streaming event whose `source.lsn` is at or below this value
    // has already been durably applied to XTDB.  Starts at 0 on fresh begin; the `> 0` guard
    // in the dedup check prevents accidentally skipping pre-first-commit events.
    //
    // NOTE: the field is currently Postgres-LSN-shaped; when MySQL arrives we'll need to
    // generalise this into a per-backend `SourcePosition`.
    private var lastAppliedLsn: Long = initialToken?.lastAppliedLsn ?: 0L

    // Our XTDB-side monotonic txId counter.  Source XIDs wrap; we mint our own.
    private var latestTxId: Long = initialToken?.latestTxId ?: -1L

    // Smoothed clock: enforces monotonic system-time across txs.  Source ts_ms is millisecond-
    // precision; back-to-back events can collide, which would give two XTDB txs the same
    // systemFrom and collapse valid-time history.
    private var lastSystemTimeMicros: Long = initialToken?.latestSystemTimeMicros ?: Long.MIN_VALUE

    override fun handleBatch(
        records: MutableList<ChangeEvent<String, String>>,
        committer: DebeziumEngine.RecordCommitter<ChangeEvent<String, String>>,
    ) {
        var streamingTx: StreamingTx? = null
        val snapshotBuf: MutableList<ParsedRecord> = mutableListOf()

        for (record in records) {
            val p = classify(record)
            if (p == null) {
                // Not a data event (heartbeat, schema change, tombstone) — ack and skip.
                committer.markProcessed(record)
                continue
            }

            // Real Postgres LSNs are > 0; the `lastAppliedLsn > 0` guard prevents deduping
            // on cold start where the high-water mark hasn't been set yet.
            if (lastAppliedLsn > 0L && p.lsn in 1..lastAppliedLsn) {
                committer.markProcessed(record)
                continue
            }

            if (p.isSnapshot) {
                streamingTx?.let {
                    flushXtdbTxn(it.events, committer, isSnapshot = false)
                    streamingTx = null
                }
                snapshotBuf.add(p)
                if (snapshotBuf.size >= snapshotChunkSize) {
                    flushXtdbTxn(snapshotBuf.toList(), committer, isSnapshot = true)
                    snapshotBuf.clear()
                }
            } else {
                if (snapshotBuf.isNotEmpty()) {
                    flushXtdbTxn(snapshotBuf.toList(), committer, isSnapshot = true)
                    snapshotBuf.clear()
                }
                val pending = streamingTx
                if (pending != null && pending.txId != p.txId) {
                    flushXtdbTxn(pending.events, committer, isSnapshot = false)
                    streamingTx = null
                }
                val accumulator = streamingTx ?: StreamingTx(p.txId, mutableListOf()).also { streamingTx = it }
                accumulator.events.add(p)
            }
        }

        // End-of-batch flush.  Snapshot: always safe.  Streaming: flushing a partial source
        // txn is rare — pgoutput delivers txns contiguously — and on retry, dedup by LSN
        // drops events we already committed.
        if (snapshotBuf.isNotEmpty()) flushXtdbTxn(snapshotBuf.toList(), committer, isSnapshot = true)
        streamingTx?.let { flushXtdbTxn(it.events, committer, isSnapshot = false) }

        committer.markBatchFinished()
    }

    // One record → ParsedRecord, or null if this isn't a data event (heartbeat, schema change,
    // tombstone).  Non-data records still get acked by the caller so Debezium advances.
    private fun classify(record: ChangeEvent<String, String>): ParsedRecord? {
        val value = record.value() ?: return null
        val payload = parseCdcEnvelope(value)
        val source = payload["source"] as? Map<*, *> ?: return null
        if (payload["op"] !is String) return null

        val lsn = (source["lsn"] as? Number)?.toLong() ?: return null
        val snapshotFlag = source["snapshot"]
        val isSnapshot = snapshotFlag != null && snapshotFlag.toString() != "false"

        return ParsedRecord(
            record = record,
            payload = payload,
            lsn = lsn,
            // Postgres emits this as "txId" in JSON; tolerate lowercase as defensive coding.
            txId = (source["txId"] as? Number)?.toLong() ?: (source["txid"] as? Number)?.toLong(),
            isSnapshot = isSnapshot,
            tsMs = (source["ts_ms"] as? Number)?.toLong() ?: 0L,
        )
    }

    private fun flushXtdbTxn(
        records: List<ParsedRecord>,
        committer: DebeziumEngine.RecordCommitter<ChangeEvent<String, String>>,
        isSnapshot: Boolean,
    ) {
        if (records.isEmpty()) return

        val maxTsMs = records.maxOf { it.tsMs }
        val tsMicros = maxOf(
            Instant.ofEpochMilli(maxTsMs).let { it.epochSecond * 1_000_000L + it.nano / 1_000L },
            lastSystemTimeMicros + 1,
        )
        val systemTime = InstantUtil.fromMicros(tsMicros)
        val txId = ++latestTxId
        val txKey = TransactionKey(txId, systemTime)
        val maxEventLsn = records.maxOf { it.lsn }

        val offsetStore = DebeziumEngineOffsetStore.handleFor(offsetStoreId)
        val currentOffsets = offsetStore?.snapshotEntries().orEmpty()

        val resumeToken: ExternalSourceToken = ProtoAny.pack(
            debeziumEngineOffsetToken {
                debeziumOffsets += currentOffsets
                lastAppliedLsn = if (isSnapshot) this@ChangeConsumer.lastAppliedLsn else maxEventLsn
                latestTxId = txId
                latestSystemTimeMicros = tsMicros
            },
            "xtdb.debezium",
        )

        OpenTx(allocator, txKey).use { openTx ->
            for (p in records) writeCdcPayload(p.payload, dbName, openTx)
            openTx.addTxRow(dbName, openTx.txKey, null)
            runBlocking { txHandler.handleTx(openTx, resumeToken) }
        }

        // Promote in-memory watermarks only after `handleTx` has returned successfully,
        // outside the `use` block so a close-throws can't leave them ahead of durable state.
        lastSystemTimeMicros = tsMicros
        if (!isSnapshot) lastAppliedLsn = maxEventLsn

        for (p in records) committer.markProcessed(p.record)
    }
}

// Parsed form of a Debezium record — decode once, pass around instead of re-parsing JSON.
private class ParsedRecord(
    val record: ChangeEvent<String, String>,
    val payload: Map<String, Any?>,
    val lsn: Long,
    val txId: Long?,
    val isSnapshot: Boolean,
    val tsMs: Long,
)

// Pairs a source txn's buffered events with its txId so they can only be set and cleared
// as a unit.  Making illegal states unrepresentable.
private class StreamingTx(val txId: Long?, val events: MutableList<ParsedRecord>)
