package xtdb.database

import xtdb.BufferPool
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.indexer.LiveIndex
import xtdb.operator.scan.ScanEmitter
import xtdb.trie.TrieCatalog

interface Database {
    val blockCatalog: BlockCatalog
    val tableCatalog: TableCatalog
    val trieCatalog: TrieCatalog

    val bufferPool: BufferPool
    val liveIndex: LiveIndex

    val scanEmitter: ScanEmitter
}