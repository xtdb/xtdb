package xtdb.database

import clojure.lang.*
import kotlinx.coroutines.*
import kotlinx.coroutines.time.withTimeout
import xtdb.util.logger
import xtdb.util.warn
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.api.TransactionResult
import xtdb.api.Xtdb
import xtdb.api.YAML_SERDE
import xtdb.api.log.IngestionStoppedException
import xtdb.api.log.Log
import xtdb.api.log.MessageId
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.api.storage.Storage.applyStorage
import xtdb.arrow.VectorType
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.garbage_collector.GarbageCollector
import xtdb.database.proto.DatabaseConfig
import xtdb.database.proto.DatabaseMode
import xtdb.indexer.*
import xtdb.metadata.PageMetadata
import xtdb.query.IQuerySource
import xtdb.storage.BufferPool
import xtdb.table.DatabaseName
import xtdb.table.TableRef
import xtdb.trie.ColumnName
import xtdb.trie.TrieCatalog
import xtdb.tx.TxOp
import xtdb.tx.TxOpts
import xtdb.tx.serializeUserMetadata
import xtdb.tx.toArrowBytes
import xtdb.util.MsgIdUtil.offsetToMsgId
import xtdb.util.closeAll
import xtdb.util.safelyOpening
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import xtdb.api.log.ReplicaMessage.ResolvedTx
import java.time.Duration
import java.util.*

private val LOG = Database::class.logger

class Database(
    val allocator: BufferAllocator,
    val config: Config,
    override val storage: DatabaseStorage,
    override val queryState: DatabaseState,
    val isIndexing: Boolean,
    val watchers: Watchers,
    private val meterRegistry: MeterRegistry?,
    val compactorOrNull: Compactor.ForDatabase? = null,
    val gcOrNull: GarbageCollector.ForDatabase? = null,
    private val job: Job? = null,
    private val processor: AutoCloseable? = null,
    private val indexerForDb: Indexer.ForDatabase? = null,
    private val registeredGauges: List<Gauge> = emptyList(),
) : IQuerySource.QueryDatabase, AutoCloseable {
    val name: DatabaseName get() = queryState.name
    override fun openSnapshot(): Snapshot = queryState.liveIndex.openSnapshot()

    val blockCatalog: BlockCatalog get() = queryState.blockCatalog
    val tableCatalog: TableCatalog get() = queryState.tableCatalog

    fun getColumnTypes(table: TableRef): Map<ColumnName, VectorType>? = tableCatalog.getTypes(table)
    val trieCatalog: TrieCatalog get() = queryState.trieCatalog
    val liveIndex: LiveIndex get() = queryState.liveIndex

    val sourceLog: Log<SourceMessage> get() = storage.sourceLog
    val replicaLog: Log<ReplicaMessage> get() = storage.replicaLog
    val bufferPool: BufferPool get() = storage.bufferPool
    val metadataManager: PageMetadata.Factory get() = storage.metadataManager

    val compactor: Compactor.ForDatabase get() = compactorOrNull ?: error("compactor not initialised")

    val latestProcessedMsgId: MessageId get() = watchers.latestSourceMsgId
    val ingestionError: IngestionStoppedException? get() = watchers.exception

    fun awaitTxBlocking(txId: Long, timeout: Duration? = null): TransactionResult? =
        runBlocking {
            check(isIndexing) { "log processor not initialised" }
            if (timeout == null) watchers.awaitTx(txId) else withTimeout(timeout) { watchers.awaitTx(txId) }
        }

    fun awaitSourceBlocking(sourceMsgId: MessageId, timeout: Duration? = null) =
        runBlocking {
            check(isIndexing) { "log processor not initialised" }
            if (timeout == null) watchers.awaitSource(sourceMsgId)
            else withTimeout(timeout) { watchers.awaitSource(sourceMsgId) }
        }

    override fun equals(other: Any?): Boolean =
        this === other || (other is Database && name == other.name)

    override fun hashCode() = Objects.hash(name)

    override fun close() {
        meterRegistry?.let { reg -> registeredGauges.forEach { reg.remove(it) } }
        job?.let { runBlocking { it.cancelAndJoin() } }
        listOf(processor, compactorOrNull, gcOrNull, indexerForDb, queryState, storage, allocator).closeAll()
    }

    fun submitTxBlocking(ops: List<TxOp>, opts: TxOpts): Xtdb.SubmittedTx {
        val defaultTz = checkNotNull(opts.defaultTz) { "missing defaultTz" }
        val txMsg = SourceMessage.Tx(
            txOps = ops.toArrowBytes(allocator),
            systemTime = opts.systemTime,
            defaultTz = defaultTz,
            user = opts.user,
            userMetadata = opts.userMetadata?.let { serializeUserMetadata(allocator, it) }
        )
        val meta = sourceLog.appendMessageBlocking(txMsg)
        return Xtdb.SubmittedTx(meta.msgId)
    }

    fun sendFlushBlockMessage(): Log.MessageMetadata = runBlocking {
        sourceLog.appendMessage(SourceMessage.FlushBlock(blockCatalog.currentBlockIndex ?: -1))
    }

    fun sendAttachDbMessage(dbName: DatabaseName, config: Config): Log.MessageMetadata = runBlocking {
        sourceLog.appendMessage(SourceMessage.AttachDatabase(dbName, config))
    }

    fun sendDetachDbMessage(dbName: DatabaseName): Log.MessageMetadata = runBlocking {
        sourceLog.appendMessage(SourceMessage.DetachDatabase(dbName))
    }

    companion object {
        private val singleWriter: Boolean =
            System.getenv("XTDB_SINGLE_WRITER")?.toBooleanStrictOrNull() ?: false

        @JvmStatic
        fun open(
            base: NodeBase,
            dbName: DatabaseName,
            dbConfig: Config,
            indexer: Indexer,
            compactor: Compactor,
            gc: GarbageCollector,
            dbCatalog: Catalog? = null,
        ): Database = safelyOpening {
            val indexerConfig = base.config.indexer
            val readOnly = dbConfig.isReadOnly

            val allocator = open { base.allocator.newChildAllocator("database/$dbName", 0, Long.MAX_VALUE) }
            val storage = open { DatabaseStorage.open(allocator, base, dbName, dbConfig) }
            val state = open { DatabaseState.open(allocator, storage, dbName, indexerConfig) }

            val blockCatalog = state.blockCatalog
            val sourceMsgId = maxOf(
                blockCatalog.latestProcessedMsgId ?: -1,
                offsetToMsgId(storage.sourceLog.epoch, -1)
            )
            val watchers = Watchers(sourceMsgId, sourceMsgId, blockCatalog.externalSourceToken)

            val crashLogger = CrashLogger(allocator, storage.bufferPool, base.config.nodeId)
            val indexerForDb = open { indexer.openForDatabase(allocator, storage, state, state.liveIndex, crashLogger) }

            val compactorForDb = open {
                if (readOnly) Compactor.NOOP.openForDatabase(allocator, storage, state, watchers)
                else compactor.openForDatabase(allocator, storage, state, watchers)
            }

            val gcForDb = open { gc.openForDatabase(storage.bufferPool, state) }

            var processor: AutoCloseable? = null
            val job = Job()
            val scope = CoroutineScope(job + CoroutineExceptionHandler { _, e ->
                watchers.notifyError(e)
            })

            if (indexerConfig.enabled) {
                if (singleWriter) {
                    val blockUploader = BlockUploader(storage, state, compactorForDb, dbCatalog)

                    val procFactory = object : LogProcessor.ProcessorFactory {
                        override fun openFollower(
                            pendingBlock: PendingBlock?,
                            afterSourceMsgId: MessageId,
                            afterReplicaMsgId: MessageId,
                        ) = FollowerLogProcessor(
                            allocator, storage.bufferPool, state, compactorForDb,
                            watchers, dbCatalog, pendingBlock, afterSourceMsgId, afterReplicaMsgId
                        )

                        override fun openLeaderSystem(
                            replicaProducer: Log.AtomicProducer<ReplicaMessage>,
                            afterSourceMsgId: MessageId,
                            afterReplicaMsgId: MessageId,
                        ): LogProcessor.LeaderSystem {
                            val leaderProc = LeaderLogProcessor(
                                allocator, storage, replicaProducer, state,
                                indexerForDb, watchers,
                                indexerConfig.skipTxs.toSet(),
                                dbCatalog, blockUploader,
                                afterSourceMsgId, afterReplicaMsgId,
                                indexerConfig.flushDuration,
                                base.meterRegistry,
                            )

                            val extFactory = dbConfig.externalSource

                            return if (extFactory != null) {
                                val extSource = extFactory.open(dbName, base.logClusters)
                                val liveIndex = state.liveIndex

                                val txHandler = ExternalSource.TxHandler { openTx, resumeToken ->
                                    val tableData = openTx.serializeTableData()
                                    liveIndex.commitTx(openTx)
                                    leaderProc.handleExternalTx(ResolvedTx(
                                        txId = openTx.txKey.txId, systemTime = openTx.txKey.systemTime,
                                        committed = null, error = null, tableData = tableData,
                                        externalSourceToken = resumeToken,
                                    ))
                                }

                                val afterToken = state.blockCatalog.externalSourceToken
                                val demux = ExternalSource.Demux(leaderProc, extSource, 0, afterToken, txHandler)
                                object : LogProcessor.LeaderSystem {
                                    override val proc get() = demux
                                    override fun isProducerFenced(e: Throwable) = replicaProducer.isProducerFenced(e)
                                    override fun close() { demux.close(); extSource.close(); leaderProc.close(); replicaProducer.close() }
                                }
                            } else {
                                object : LogProcessor.LeaderSystem {
                                    override val proc get() = leaderProc
                                    override fun isProducerFenced(e: Throwable) = replicaProducer.isProducerFenced(e)
                                    override fun close() { leaderProc.close(); replicaProducer.close() }
                                }
                            }
                        }

                        override fun openTransition(
                            replicaProducer: Log.AtomicProducer<ReplicaMessage>,
                            afterSourceMsgId: MessageId,
                            afterReplicaMsgId: MessageId,
                        ) = TransitionLogProcessor(
                            allocator, storage.bufferPool, state, state.liveIndex,
                            blockUploader, replicaProducer,
                            watchers, dbCatalog,
                            afterSourceMsgId, afterReplicaMsgId
                        )
                    }

                    val lp = LogProcessor(procFactory, storage, state, blockUploader, scope, base.meterRegistry)
                    processor = lp

                    if (!readOnly) {
                        scope.launch { storage.sourceLog.openGroupSubscription(lp) }
                    }
                } else {
                    val srcProc = SourceLogProcessor(
                        allocator, base.meterRegistry ?: error("no meter registry"),
                        storage, state,
                        indexerForDb, state.liveIndex,
                        watchers, compactorForDb,
                        indexerConfig.skipTxs.toSet(),
                        readOnly, dbCatalog,
                        indexerConfig.flushDuration,
                    )
                    processor = srcProc

                    val latestProcessedMsgId = blockCatalog.latestProcessedMsgId ?: -1
                    scope.launch { storage.sourceLog.tailAll(latestProcessedMsgId, srcProc) }
                }
            }

            val meterRegistry = base.meterRegistry
            val gauges = meterRegistry?.let { reg ->
                fun gauge(name: String, f: () -> Double) =
                    Gauge.builder(name) { f() }
                        .tag("db", dbName)
                        .register(reg)

                listOf(
                    gauge("node.tx.latestCompletedTxId") {
                        (state.liveIndex.latestCompletedTx?.txId ?: -1).toDouble()
                    },
                    gauge("node.tx.latestSubmittedMsgId") {
                        storage.sourceLog.latestSubmittedMsgId.toDouble()
                    },
                    gauge("node.tx.latestProcessedMsgId") {
                        watchers.latestSourceMsgId.toDouble()
                    },
                    gauge("node.tx.lag.MsgId") {
                        maxOf(storage.sourceLog.latestSubmittedMsgId - watchers.latestSourceMsgId, 0).toDouble()
                    },
                )
            } ?: emptyList()

            Database(
                allocator = allocator,
                config = dbConfig,
                storage = storage,
                queryState = state,
                isIndexing = indexerConfig.enabled,
                watchers = watchers,
                meterRegistry = meterRegistry,
                compactorOrNull = compactorForDb,
                gcOrNull = gcForDb,
                job = job,
                processor = processor,
                indexerForDb = indexerForDb,
                registeredGauges = gauges,
            )
        }
    }

    @Serializable
    enum class Mode {
        @SerialName("read-write")
        READ_WRITE,

        @SerialName("read-only")
        READ_ONLY;

        fun toProto(): DatabaseMode = when (this) {
            READ_WRITE -> DatabaseMode.READ_WRITE
            READ_ONLY -> DatabaseMode.READ_ONLY
        }

        companion object {
            @JvmStatic
            fun fromProto(mode: DatabaseMode): Mode = when (mode) {
                DatabaseMode.READ_WRITE, DatabaseMode.UNRECOGNIZED -> READ_WRITE
                DatabaseMode.READ_ONLY -> READ_ONLY
            }
        }
    }

    @Serializable
    data class Config(
        val log: Log.Factory = Log.inMemoryLog,
        val storage: Storage.Factory = Storage.inMemory(),
        val mode: Mode = Mode.READ_WRITE,
        val externalSource: ExternalSource.Factory? = null,
        val critical: Boolean = false,
    ) {
        fun log(log: Log.Factory) = copy(log = log)
        fun storage(storage: Storage.Factory) = copy(storage = storage)
        fun mode(mode: Mode) = copy(mode = mode)
        fun externalSource(externalSource: ExternalSource.Factory?) = copy(externalSource = externalSource)
        fun critical(critical: Boolean) = copy(critical = critical)

        val isReadOnly: Boolean get() = mode == Mode.READ_ONLY

        val serializedConfig: DatabaseConfig
            get() = DatabaseConfig.newBuilder()
                .also { dbConfig ->
                    log.writeTo(dbConfig)
                    dbConfig.applyStorage(storage)
                    dbConfig.mode = mode.toProto()
                    externalSource?.writeTo(dbConfig)
                    dbConfig.critical = critical
                }.build()

        companion object {
            @JvmStatic
            fun fromYaml(yaml: String): Config = YAML_SERDE.decodeFromString(yaml.trimIndent())

            @JvmStatic
            fun fromProto(dbConfig: DatabaseConfig) =
                Config()
                    .log(Log.Factory.fromProto(dbConfig))
                    .storage(Storage.Factory.fromProto(dbConfig))
                    .mode(Mode.fromProto(dbConfig.mode))
                    .externalSource(ExternalSource.Factory.fromProto(dbConfig))
                    .critical(dbConfig.critical)
        }
    }

    interface Catalog : ILookup, Seqable, Iterable<Database>, IQuerySource.QueryCatalog {
        companion object {
            private suspend fun Database.awaitSource(msgId: MessageId) = watchers.awaitSource(msgId)
            private suspend fun Database.sync() = watchers.awaitSource(sourceLog.latestSubmittedMsgId)

            private suspend fun Catalog.awaitAll0(token: String) = coroutineScope {
                val basis = token.decodeTxBasisToken()

                databaseNames
                    .mapNotNull { databaseOrNull(it) }
                    .filter { it.isIndexing }
                    .map { db -> launch { basis[db.name]?.first()?.let { db.awaitSource(it) } } }
                    .joinAll()
            }

            private suspend fun Catalog.syncAll0() = coroutineScope {
                databaseNames
                    .mapNotNull { databaseOrNull(it) }
                    .filter { it.isIndexing }
                    .map { db -> launch { db.sync() } }
                    .joinAll()
            }

            @JvmField
            val EMPTY = object : Catalog {
                override val databaseNames: Collection<DatabaseName> = emptySet()

                override fun databaseOrNull(dbName: DatabaseName) = null

                override fun attach(dbName: DatabaseName, config: Config?): Unit =
                    error("can't attach database to empty db-cat")

                override fun detach(dbName: DatabaseName) =
                    error("can't detach database from empty db-cat")
            }
        }

        override val databaseNames: Collection<DatabaseName>
        override fun databaseOrNull(dbName: DatabaseName): Database?

        operator fun get(dbName: DatabaseName) = databaseOrNull(dbName)

        val primary: Database get() = databaseOrNull("xtdb")!!

        fun attach(dbName: DatabaseName, config: Config?)
        fun detach(dbName: DatabaseName)

        override fun valAt(key: Any?) = valAt(key, null)
        override fun valAt(key: Any?, notFound: Any?) = databaseOrNull(key as DatabaseName) ?: notFound

        override fun iterator() = databaseNames.mapNotNull { databaseOrNull(it) }.iterator()

        override fun seq(): ISeq? =
            databaseNames.takeIf { it.isNotEmpty() }
                ?.map { MapEntry(it, databaseOrNull(it)) }
                ?.let { RT.seq(it) }

        fun awaitAll(token: String?, timeout: Duration?) = runBlocking {
            if (token != null)
                try {
                    if (timeout == null) awaitAll0(token) else withTimeout(timeout) { awaitAll0(token) }
                } catch (e: TimeoutCancellationException) {
                    val basis = token.decodeTxBasisToken()
                    val dbStatus = databaseNames
                        .mapNotNull { databaseOrNull(it) }
                        .filter { it.isIndexing }
                        .joinToString("\n") { db ->
                            val awaiting = basis[db.name]?.first()
                            val current = db.latestProcessedMsgId
                            val error = db.ingestionError != null
                            "  db=${db.name}: awaiting=$awaiting, current=$current, ingestionError=$error"
                        }
                    LOG.warn("awaitAll timed out after $timeout:\n$dbStatus")
                    throw e
                }
        }

        fun syncAll(timeout: Duration?) = runBlocking {
            if (timeout == null) syncAll0() else withTimeout(timeout) { syncAll0() }
        }

        val serialisedSecondaryDatabases: Map<DatabaseName, DatabaseConfig>
            get() = this.filterNot { it.name == "xtdb" }
                .associate { db -> db.name to db.config.serializedConfig }
    }
}
