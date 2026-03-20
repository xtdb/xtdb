package xtdb.garbage_collector

import kotlinx.coroutines.*
import xtdb.catalog.BlockCatalog.Companion.blockFromLatest
import xtdb.database.DatabaseState
import xtdb.storage.BufferPool
import xtdb.table.TableRef
import xtdb.time.microsAsInstant
import xtdb.trie.Trie.dataFilePath
import xtdb.trie.Trie.metaFilePath
import xtdb.trie.TrieKey
import xtdb.util.*
import java.io.Closeable
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import kotlin.coroutines.CoroutineContext
import kotlin.random.Random
import kotlin.time.Duration.Companion.seconds

private val LOGGER = GarbageCollector::class.logger

interface GarbageCollector : Closeable {
    fun openForDatabase(bufferPool: BufferPool, dbState: DatabaseState): ForDatabase

    interface ForDatabase : Closeable {
        suspend fun garbageCollectTries(garbageAsOf: Instant? = null)
        suspend fun collectGarbage()
        fun collectAllGarbage()
    }

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

    class ForDatabaseImpl @JvmOverloads constructor(
        private val bufferPool: BufferPool,
        dbState: DatabaseState,
        driverFactory: Driver.Factory,
        private val blocksToKeep: Int,
        private val garbageLifetime: Duration,
        private val approxRunInterval: Duration,
        enabled: Boolean,
        private val coroutineCtx: CoroutineContext = Dispatchers.IO
    ) : ForDatabase {
        private val parentJob = Job()
        private val driver = driverFactory.create(bufferPool, dbState)
        private val blockCatalog = dbState.blockCatalog
        private val trieCatalog = dbState.trieCatalog
        private val blockGc = BlockGarbageCollector(blockCatalog, bufferPool, blocksToKeep)

        init {
            if (enabled) {
                LOGGER.debug { "Starting GarbageCollector for database with approxRunInterval: $approxRunInterval, blocksToKeep: $blocksToKeep" }

                CoroutineScope(coroutineCtx + parentJob).launch {
                    delay(Random.nextLong(approxRunInterval.toMillis()))

                    while (isActive) {
                        try {
                            LOGGER.debugBlock("garbage collection cycle") {
                                collectGarbage()
                            }
                        } catch (_: Exception) { /* already logged */ }

                        delay(approxRunInterval.toMillis())
                    }
                }
            }
        }

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

            LOGGER.debug { "Garbage collecting data older than $asOf" }
            yieldIfSimulation()
            val tableNames = blockCatalog.allTables.shuffled().take(100)
            for (tableName in tableNames) {
                val garbageTries = trieCatalog.garbageTries(tableName, asOf)
                for (garbageTrie in garbageTries) {
                    driver.deletePath(tableName.metaFilePath(garbageTrie))
                    driver.deletePath(tableName.dataFilePath(garbageTrie))
                }
                driver.deleteTries(tableName, garbageTries)
            }
        }

        override suspend fun collectGarbage() {
            runInterruptible { blockGc.garbageCollectBlocks() }
            garbageCollectTries()
        }

        override fun collectAllGarbage() = runBlocking {
            collectGarbage()
        }

        override fun close() {
            runBlocking {
                withTimeoutOrNull(5.seconds) { parentJob.cancelAndJoin() }
                    ?: LOGGER.warn("GC coroutine did not stop within 5s")
            }
            driver.close()
        }
    }

    class Impl @JvmOverloads constructor(
        private val driverFactory: Driver.Factory,
        private val blocksToKeep: Int,
        private val garbageLifetime: Duration,
        private val approxRunInterval: Duration,
        private val enabled: Boolean,
        private val coroutineCtx: CoroutineContext = Dispatchers.IO
    ) : GarbageCollector {

        override fun openForDatabase(bufferPool: BufferPool, dbState: DatabaseState): ForDatabase =
            ForDatabaseImpl(bufferPool, dbState, driverFactory, blocksToKeep, garbageLifetime, approxRunInterval, enabled, coroutineCtx)

        override fun close() {}
    }
}
