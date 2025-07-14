package xtdb.database

import org.apache.arrow.memory.BufferAllocator
import xtdb.BufferPool
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.indexer.LiveIndex
import xtdb.operator.scan.ScanEmitter
import xtdb.trie.TrieCatalog

typealias DatabaseName = String

interface Database : AutoCloseable {
    val name: DatabaseName

    val allocator: BufferAllocator

    val blockCatalog: BlockCatalog
    val tableCatalog: TableCatalog
    val trieCatalog: TrieCatalog

    val bufferPool: BufferPool
    val liveIndex: LiveIndex

    val scanEmitter: ScanEmitter
}