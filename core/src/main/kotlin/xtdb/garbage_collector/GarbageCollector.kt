package xtdb.garbage_collector

import kotlinx.coroutines.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.plus
import org.slf4j.LoggerFactory
import xtdb.catalog.BlockCatalog.Companion.blockFromLatest
import xtdb.database.DatabaseState
import xtdb.storage.BufferPool
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
            fun create(bufferPool: BufferPool, dbState: DatabaseState): Driver
        }

        companion object {
            @JvmStatic
            fun real() = object : Factory {
                override fun create(bufferPool: BufferPool, dbState: DatabaseState) = object : Driver {
                    private val trieCatalog = dbState.trieCatalog

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
        private val bufferPool: BufferPool,
        dbState: DatabaseState,
        driverFactory: Driver.Factory,
        private val blocksToKeep: Int,
        private val garbageLifetime: Duration,
        private val approxRunInterval: Duration,
        private val coroutineCtx: CoroutineContext = Dispatchers.IO
    ) : GarbageCollector {
        private val parentJob = Job()
        private val driver = driverFactory.create(bufferPool, dbState)
        private val blockCatalog = dbState.blockCatalog
        private val trieCatalog = dbState.trieCatalog
        private val blockGc = BlockGarbageCollector(blockCatalog, bufferPool, blocksToKeep)

        private fun defaultGarbageAsOf(): Instant? =
            bufferPool.blockFromLatest(blocksToKeep)
                ?.let { it.latestCompletedTx.systemTime.microsAsInstant - garbageLifetime }

        // For testing
        @OptIn(ExperimentalStdlibApi::class)
        private suspend fun yieldIfSimulation() {
            if (coroutineCtx[CoroutineDispatcher.Key] != Dispatchers.IO) yield()
        }

        override suspend fun garbageCollectTries(garbageAsOf: Instant?) {
            val asOf = garbageAsOf ?: defaultGarbageAsOf() ?: return

            LOGGER.debug("Garbage collecting data older than {}", asOf)

            try {
                yieldIfSimulation() // simulate suspension for testing
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

