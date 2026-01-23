package xtdb.garbage_collector

import kotlinx.coroutines.yield
import xtdb.api.log.Log.Message
import xtdb.database.IDatabase
import xtdb.table.TableRef
import xtdb.trie.TrieKey
import xtdb.util.debug
import xtdb.util.logger
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

private val LOGGER = GarbageCollectorMockDriver::class.logger

class GarbageCollectorMockDriver() : GarbageCollector.Driver.Factory {
    var nextSystemId = 0

    // Track all deleted paths and trie keys across all systems
    val deletedPaths = mutableListOf<Path>()
    val deletedTrieKeys = ConcurrentHashMap<TableRef, MutableSet<TrieKey>>()

    override fun create(db: IDatabase) = ForDatabase(db, nextSystemId++)

    inner class ForDatabase(private val db: IDatabase, val systemId: Int) : GarbageCollector.Driver {
        private val bufferPool = db.bufferPool
        private val trieCatalog = db.trieCatalog

        override suspend fun deletePath(path: Path) {
            yield()
            LOGGER.debug("systemId=$systemId Deleting path $path")
            synchronized(deletedPaths) { deletedPaths.add(path) }
            bufferPool.deleteIfExists(path)
        }

        override suspend fun appendMessage(msg: Message.TriesDeleted) {
            yield()

            val table = msg.tableName.let { TableRef.parse(db.name, it) }

            deletedTrieKeys.compute(table) { _, existingKeys ->
                val keys = existingKeys ?: mutableSetOf()
                keys.addAll(msg.trieKeys)
                keys
            }

            trieCatalog.deleteTries(table, msg.trieKeys.toSet())
        }

        override fun close() {}
    }
}
