package xtdb.garbage_collector

import kotlinx.coroutines.yield
import xtdb.database.IDatabase
import xtdb.table.TableRef
import xtdb.trie.TrieKey
import xtdb.util.debug
import xtdb.util.logger
import java.nio.file.Path

private val LOGGER = GarbageCollectorMockDriver::class.logger

class GarbageCollectorMockDriver() : GarbageCollector.Driver.Factory {
    override fun create(db: IDatabase) = ForDatabase(db)

    class ForDatabase(val db: IDatabase) : GarbageCollector.Driver {
        private val bufferPool = db.bufferPool
        private val trieCatalog = db.trieCatalog

        override suspend fun deletePath(path: Path) {
            yield()
            LOGGER.debug("Deleting path $path")
            bufferPool.deleteIfExists(path)
        }

        override suspend fun deleteTries(tableName: TableRef, trieKeys: Set<TrieKey>) {
            yield()
            LOGGER.debug("Removing ${trieKeys.size} tries from catalog for table $tableName")
            trieCatalog.deleteTries(tableName, trieKeys)
        }

        override fun close() {}
    }
}
