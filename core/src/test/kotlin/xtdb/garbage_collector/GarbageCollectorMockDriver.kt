package xtdb.garbage_collector

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.yield
import org.slf4j.LoggerFactory
import xtdb.database.IDatabase
import xtdb.table.TableRef
import xtdb.trie.Trie.dataFilePath
import xtdb.trie.Trie.metaFilePath
import xtdb.trie.TrieKey
import java.time.Instant

private val LOGGER = LoggerFactory.getLogger(GarbageCollectorMockDriver::class.java)

class GarbageCollectorMockDriver() : GarbageCollector.Driver.Factory {
    override fun create(db: IDatabase) = ForDatabase(db)

    class ForDatabase(val db: IDatabase) : GarbageCollector.Driver {
        private val bufferPool = db.bufferPool
        private val trieCatalog = db.trieCatalog

        override suspend fun deleteGarbageTries(tableName: TableRef, garbageTries: Set<TrieKey>) {
            for (garbageTrie in garbageTries) {
                LOGGER.debug("Deleting data file for garbage trie {}", garbageTrie)
                bufferPool.deleteIfExists(tableName.metaFilePath(garbageTrie))
                yield()
                LOGGER.debug("Deleting meta file for garbage trie {}", garbageTrie)
                bufferPool.deleteIfExists(tableName.dataFilePath(garbageTrie))
                yield()
            }
            LOGGER.debug("Removing {} garbage tries from catalog for table {}", garbageTries.size, tableName)
            trieCatalog.deleteTries(tableName, garbageTries)
        }

        override fun close() {}
    }
}
