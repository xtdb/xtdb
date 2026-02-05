package xtdb.garbage_collector

import kotlinx.coroutines.yield
import xtdb.database.DatabaseState
import xtdb.storage.BufferPool
import xtdb.table.TableRef
import xtdb.trie.TrieKey
import xtdb.util.debug
import xtdb.util.logger
import java.nio.file.Path

private val LOGGER = GarbageCollectorMockDriver::class.logger

class GarbageCollectorMockDriver() : GarbageCollector.Driver.Factory {
    var nextSystemId = 0

    // Track all deleted paths and trie keys across all systems
    val deletedPaths = mutableListOf<Path>()
    val deletedTrieKeys = mutableMapOf<TableRef, MutableSet<TrieKey>>()

    override fun create(bufferPool: BufferPool, dbState: DatabaseState) = ForDatabase(bufferPool, dbState, nextSystemId++)

    inner class ForDatabase(val bufferPool: BufferPool, val dbState: DatabaseState, val systemId: Int) : GarbageCollector.Driver {
        private val trieCatalog = dbState.trieCatalog

        override suspend fun deletePath(path: Path) {
            yield()
            LOGGER.debug("systemId=$systemId Deleting path $path")
            synchronized(deletedPaths) { deletedPaths.add(path) }
            bufferPool.deleteIfExists(path)
        }

        override suspend fun deleteTries(tableName: TableRef, trieKeys: Set<TrieKey>) {
            yield()
            LOGGER.debug("systemId=$systemId Removing ${trieKeys.size} tries from catalog for table $tableName: $trieKeys")
            synchronized(deletedTrieKeys) {
                deletedTrieKeys.getOrPut(tableName) { mutableSetOf() }.addAll(trieKeys)
            }
            trieCatalog.deleteTries(tableName, trieKeys)
        }

        override fun close() {}
    }
}
