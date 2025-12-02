package xtdb.garbage_collector

import kotlinx.coroutines.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.plus
import org.slf4j.LoggerFactory
import xtdb.database.IDatabase
import xtdb.table.TableRef
import xtdb.time.microsAsInstant
import xtdb.trie.Trie.dataFilePath
import xtdb.trie.Trie.metaFilePath
import xtdb.trie.TrieKey
import java.io.Closeable
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import kotlin.coroutines.CoroutineContext
import kotlin.random.Random
import kotlin.time.Duration.Companion.seconds

private val LOGGER = LoggerFactory.getLogger(GarbageCollector::class.java)


interface GarbageCollector : Closeable {
    suspend fun garbageCollectTries(garbageAsOf: Instant? = null)
    suspend fun collectGarbage()
    fun collectAllGarbage()
    fun start()

    interface Driver : AutoCloseable {
        suspend fun deletePath(path: Path)
        suspend fun deleteTries(tableName: TableRef, trieKeys: Set<TrieKey>)

        interface Factory {
            fun create(db: IDatabase): Driver
        }

        companion object {
            @JvmStatic
            fun real() = object : Factory {
                override fun create(db: IDatabase) = object : Driver {
                    private val trieCatalog = db.trieCatalog
                    private val bufferPool = db.bufferPool

                    override suspend fun deletePath(path: Path) {
                        bufferPool.deleteIfExists(path)
                    }

                    override suspend fun deleteTries(tableName: TableRef, trieKeys: Set<TrieKey>) {
                        trieCatalog.deleteTries(tableName, trieKeys)
                    }

                    override fun close() {}
                }
            }
        }
    }

    class Impl @JvmOverloads constructor(
        db: IDatabase,
        driverFactory: Driver.Factory,
        private val blocksToKeep: Int,
        private val garbageLifetime: Duration,
        private val approxRunInterval: Duration,
        private val coroutineCtx: CoroutineContext = Dispatchers.IO
    ) : GarbageCollector {
        private val parentJob = Job()
        private val driver = driverFactory.create(db)
        private val blockCatalog = db.blockCatalog
        private val trieCatalog = db.trieCatalog
        private val bufferPool = db.bufferPool
        private val blockGc = BlockGarbageCollector(blockCatalog, bufferPool, blocksToKeep)

        private fun defaultGarbageAsOf(): Instant? =
            blockCatalog.blockFromLatest(blocksToKeep)
                ?.let { it.latestCompletedTx.systemTime.microsAsInstant - garbageLifetime }

        override suspend fun garbageCollectTries(garbageAsOf: Instant?) {
            val asOf = garbageAsOf ?: defaultGarbageAsOf() ?: return

            LOGGER.debug("Garbage collecting data older than {}", asOf)

            try {
                LOGGER.debug("Starting trie garbage collection")
                val tableNames = blockCatalog.allTables.shuffled().take(100)
                for (tableName in tableNames) {
                    val garbageTries = trieCatalog.garbageTries(tableName, asOf)
                    for (garbageTrie in garbageTries) {
                        driver.deletePath(tableName.metaFilePath(garbageTrie))
                        driver.deletePath(tableName.dataFilePath(garbageTrie))
                    }
                    driver.deleteTries(tableName, garbageTries)
                }
                LOGGER.debug("Trie garbage collection completed")
            } catch (e: Exception) {
                LOGGER.warn("Trie garbage collection failed", e)
            } catch (e: Throwable) {
                throw RuntimeException("Error encountered during Trie garbage collection: ", e)
            }
        }

        override suspend fun collectGarbage() {
            LOGGER.debug("Starting block garbage collection")
            blockGc.garbageCollectBlocks()
            LOGGER.debug("Block garbage collection completed")

            garbageCollectTries()
            LOGGER.debug("Next GC run scheduled in ${approxRunInterval.toMillis()}ms")
        }

        override fun collectAllGarbage() = runBlocking {
            collectGarbage()
        }

        override fun start() {
            LOGGER.debug(
                "Starting GarbageCollector with approxRunInterval: {}, blocksToKeep: {}",
                approxRunInterval, blocksToKeep
            )

            CoroutineScope(coroutineCtx + parentJob).launch {
                delay(Random.nextLong(approxRunInterval.toMillis()))
                while (isActive) {
                    collectGarbage()
                    delay(approxRunInterval.toMillis())
                }
            }
        }

        override fun close() {
            runBlocking { withTimeout(5.seconds) { parentJob.cancelAndJoin() } }
            LOGGER.debug("GarbageCollector shut down")
        }
    }
}

