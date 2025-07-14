package xtdb.database

import org.apache.arrow.memory.BufferAllocator
import xtdb.BufferPool
import xtdb.api.log.Log
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.indexer.LiveIndex
import xtdb.metadata.PageMetadata
import xtdb.trie.TrieCatalog

typealias DatabaseName = String

class Database(
    val name: DatabaseName,
    val allocator: BufferAllocator,
    val blockCatalog: BlockCatalog, val tableCatalog: TableCatalog, val trieCatalog: TrieCatalog,
    val log: Log, val bufferPool: BufferPool,
    val metadataManager: PageMetadata.Factory, val liveIndex: LiveIndex
) : AutoCloseable {

    override fun close() {
        allocator.close()
    }
}