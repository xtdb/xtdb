package xtdb.garbage_collector

import kotlinx.coroutines.yield
import xtdb.database.IDatabase
import xtdb.table.TableRef
import xtdb.trie.Trie.dataFilePath
import xtdb.trie.Trie.metaFilePath
import xtdb.trie.TrieKey
import xtdb.util.debug
import xtdb.util.logger

private val LOGGER = GarbageCollectorMockDriver::class.logger

class GarbageCollectorMockDriver() : GarbageCollector.Driver.Factory {
    override fun create(db: IDatabase) = ForDatabase(db)

    class ForDatabase(val db: IDatabase) : GarbageCollector.Driver {
        private val bufferPool = db.bufferPool
        private val trieCatalog = db.trieCatalog

        override suspend fun deleteGarbageTries(tableName: TableRef, garbageTries: Set<TrieKey>) {
            for (garbageTrie in garbageTries) {
                LOGGER.debug("Deleting data file for garbage trie $garbageTrie")
                bufferPool.deleteIfExists(tableName.metaFilePath(garbageTrie))
                yield()
                LOGGER.debug("Deleting meta file for garbage trie $garbageTrie")
                bufferPool.deleteIfExists(tableName.dataFilePath(garbageTrie))
                yield()
            }
            LOGGER.debug("Removing ${garbageTries.size} garbage tries from catalog for table $tableName")
            trieCatalog.deleteTries(tableName, garbageTries)
        }

        override fun close() {}
    }
}
