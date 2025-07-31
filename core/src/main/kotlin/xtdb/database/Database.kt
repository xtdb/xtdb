package xtdb.database

import kotlinx.serialization.Serializable
import org.apache.arrow.memory.BufferAllocator
import xtdb.BufferPool
import xtdb.api.log.Log
import xtdb.api.storage.Storage
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.indexer.LiveIndex
import xtdb.indexer.LogProcessor
import xtdb.indexer.Snapshot
import xtdb.metadata.PageMetadata
import xtdb.trie.TrieCatalog
import java.util.*

typealias DatabaseName = String

data class Database(
    val name: DatabaseName,

    val allocator: BufferAllocator,
    val blockCatalog: BlockCatalog, val tableCatalog: TableCatalog, val trieCatalog: TrieCatalog,
    val log: Log, val bufferPool: BufferPool,

    // snapSource will mostly be the same as liveIndex - exception being within a transaction
    val metadataManager: PageMetadata.Factory, val liveIndex: LiveIndex, val snapSource: Snapshot.Source,

    private val logProcessorOrNull: LogProcessor?,
    private val compactorOrNull: Compactor.ForDatabase?,
) {
    val logProcessor: LogProcessor get() = logProcessorOrNull ?: error("log processor not initialised")
    val compactor: Compactor.ForDatabase get() = compactorOrNull ?: error("compactor not initialised")

    fun withComponents(logProcessor: LogProcessor?, compactor: Compactor.ForDatabase?) =
        copy(logProcessorOrNull = logProcessor, compactorOrNull = compactor)

    fun withSnapSource(snapSource: Snapshot.Source) = copy(snapSource = snapSource)

    override fun equals(other: Any?): Boolean =
        this === other || (other is Database && name == other.name)

    override fun hashCode() = Objects.hash(name)

    @Serializable
    data class Config(
        val log: Log.Factory = Log.inMemoryLog,
        val storage: Storage.Factory = Storage.inMemoryStorage(),
    ) {
        fun log(log: Log.Factory) = copy(log = log)
        fun storage(storage: Storage.Factory) = copy(storage = storage)
    }
}
