package xtdb.database

import org.apache.arrow.memory.BufferAllocator
import xtdb.BufferPool
import xtdb.api.log.Log
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.indexer.LiveIndex
import xtdb.indexer.LogProcessor
import xtdb.metadata.PageMetadata
import xtdb.trie.TrieCatalog
import java.util.Objects

typealias DatabaseName = String
typealias DatabasePart = Int

data class Database(
    val name: DatabaseName,
    val part: DatabasePart,

    val allocator: BufferAllocator,
    val blockCatalog: BlockCatalog, val tableCatalog: TableCatalog, val trieCatalog: TrieCatalog,
    val log: Log, val bufferPool: BufferPool,
    val metadataManager: PageMetadata.Factory, val liveIndex: LiveIndex,

    private val logProcessorOrNull: LogProcessor?,
    private val compactorOrNull: Compactor.ForDatabase?,
) {
    val logProcessor: LogProcessor get() = logProcessorOrNull ?: error("log processor not initialised")
    val compactor: Compactor.ForDatabase get() = compactorOrNull ?: error("compactor not initialised")

    fun withComponents(logProcessor: LogProcessor?, compactor: Compactor.ForDatabase?) =
        copy(logProcessorOrNull = logProcessor, compactorOrNull = compactor)

    override fun equals(other: Any?): Boolean =
        this === other || (other is Database && name == other.name && part == other.part)

    override fun hashCode() = Objects.hash(name, part)
}