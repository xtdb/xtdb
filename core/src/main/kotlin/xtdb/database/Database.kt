package xtdb.database

import clojure.lang.*
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.time.withTimeout
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.YAML_SERDE
import xtdb.api.log.Log
import xtdb.api.log.Log.Message
import xtdb.api.log.MessageId
import xtdb.api.storage.Storage
import xtdb.api.storage.Storage.applyStorage
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.proto.DatabaseConfig
import xtdb.indexer.LiveIndex
import xtdb.indexer.LogProcessor
import xtdb.indexer.Snapshot
import xtdb.indexer.Indexer.TxSink
import xtdb.metadata.PageMetadata
import xtdb.storage.BufferPool
import xtdb.trie.TrieCatalog
import java.time.Duration
import java.util.*

interface IDatabase {
    val name: DatabaseName
    val allocator: BufferAllocator
    val blockCatalog: BlockCatalog
    val tableCatalog: TableCatalog
    val trieCatalog: TrieCatalog
    val log: Log
    val bufferPool: BufferPool
    val metadataManager: PageMetadata.Factory
    val logProcessor: LogProcessor
    val compactor: Compactor.ForDatabase
    val txSink: TxSink
}

data class Database(
    override val name: DatabaseName, val config: Config,

    override val allocator: BufferAllocator,
    override val blockCatalog: BlockCatalog, override val tableCatalog: TableCatalog, override val trieCatalog: TrieCatalog,
    override val log: Log, override val bufferPool: BufferPool,

    // snapSource will mostly be the same as liveIndex - exception being within a transaction
    override val metadataManager: PageMetadata.Factory, val liveIndex: LiveIndex, val snapSource: Snapshot.Source,

    private val logProcessorOrNull: LogProcessor?,
    private val compactorOrNull: Compactor.ForDatabase?,
    private val txSinkOrNull: TxSink?,
): IDatabase {
    override val logProcessor: LogProcessor get() = logProcessorOrNull ?: error("log processor not initialised")
    override val compactor: Compactor.ForDatabase get() = compactorOrNull ?: error("compactor not initialised")
    override val txSink: TxSink get() = txSinkOrNull ?: error("tx sink not initialised")

    fun withComponents(logProcessor: LogProcessor?, compactor: Compactor.ForDatabase?, txSink: TxSink?) =
        copy(logProcessorOrNull = logProcessor, compactorOrNull = compactor, txSinkOrNull = txSink)

    fun withSnapSource(snapSource: Snapshot.Source) = copy(snapSource = snapSource)

    override fun equals(other: Any?): Boolean =
        this === other || (other is Database && name == other.name)

    override fun hashCode() = Objects.hash(name)

    fun sendFlushBlockMessage(): Log.MessageMetadata = runBlocking {
        log.appendMessage(Message.FlushBlock(blockCatalog.currentBlockIndex ?: -1)).await()
    }

    fun sendAttachDbMessage(dbName: DatabaseName, config: Database.Config): Log.MessageMetadata = runBlocking {
        log.appendMessage(Message.AttachDatabase(dbName, config)).await()
    }

    fun sendDetachDbMessage(dbName: DatabaseName): Log.MessageMetadata = runBlocking {
        log.appendMessage(Message.DetachDatabase(dbName)).await()
    }

    @Serializable
    data class Config(
        val log: Log.Factory = Log.inMemoryLog,
        val storage: Storage.Factory = Storage.inMemory(),
    ) {
        fun log(log: Log.Factory) = copy(log = log)
        fun storage(storage: Storage.Factory) = copy(storage = storage)

        val serializedConfig: DatabaseConfig
            get() = DatabaseConfig.newBuilder()
                .also { dbConfig ->
                    log.writeTo(dbConfig)
                    dbConfig.applyStorage(storage)
                }.build()

        companion object {
            @JvmStatic
            fun fromYaml(yaml: String): Config = YAML_SERDE.decodeFromString(yaml.trimIndent())

            @JvmStatic
            fun fromProto(dbConfig: DatabaseConfig) =
                Config()
                    .log(Log.Factory.fromProto(dbConfig))
                    .storage(Storage.Factory.fromProto(dbConfig))
        }
    }

    interface Catalog : ILookup, Seqable, Iterable<Database> {
        companion object {
            private suspend fun Database.await(msgId: MessageId) = logProcessor.awaitAsync(msgId).await()
            private suspend fun Database.sync() = await(logProcessor.latestSubmittedMsgId)

            private suspend fun Catalog.awaitAll0(token: String) = coroutineScope {
                val basis = token.decodeTxBasisToken()

                databaseNames
                    .mapNotNull { databaseOrNull(it) }
                    .map { db -> launch { basis[db.name]?.first()?.let { db.await(it) } } }
                    .joinAll()
            }

            private suspend fun Catalog.syncAll0() = coroutineScope {
                databaseNames
                    .mapNotNull { databaseOrNull(it) }
                    .map { db -> launch { db.sync() } }
                    .joinAll()
            }

            @JvmStatic
            fun singleton(db: Database) = object : Catalog {
                override val databaseNames: Collection<DatabaseName> get() = setOf(db.name)

                override fun databaseOrNull(dbName: DatabaseName) = db.takeIf { dbName == it.name }

                override fun attach(dbName: DatabaseName, config: Config?) =
                    error("can't attach database to singleton db-cat")

                override fun detach(dbName: DatabaseName) =
                    error("can't detach database from singleton db-cat")
            }

            @JvmField
            val EMPTY = object : Catalog {
                override val databaseNames: Collection<DatabaseName> = emptySet()

                override fun databaseOrNull(dbName: DatabaseName) = null

                override fun attach(dbName: DatabaseName, config: Config?) =
                    error("can't attach database to empty db-cat")

                override fun detach(dbName: DatabaseName) =
                    error("can't detach database from empty db-cat")
            }
        }

        val databaseNames: Collection<DatabaseName>
        fun databaseOrNull(dbName: DatabaseName): Database?

        operator fun get(dbName: DatabaseName) = databaseOrNull(dbName)

        val primary: Database get() = this["xtdb"]!!

        fun attach(dbName: DatabaseName, config: Config?): Database
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
                if (timeout == null) awaitAll0(token) else withTimeout(timeout) { awaitAll0(token) }
        }

        fun syncAll(timeout: Duration?) = runBlocking {
            if (timeout == null) syncAll0() else withTimeout(timeout) { syncAll0() }
        }

        val serialisedSecondaryDatabases
            get(): Map<DatabaseName, DatabaseConfig> =
                this.filterNot { it.name == "xtdb" }
                    .associate { db -> db.name to db.config.serializedConfig }
    }
}
