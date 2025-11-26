package xtdb.garbage_collector

import kotlinx.coroutines.*
import kotlinx.coroutines.CoroutineScope
import org.slf4j.LoggerFactory
import xtdb.database.Database
import xtdb.time.microsAsInstant
import xtdb.trie.Trie.dataFilePath
import xtdb.trie.Trie.metaFilePath
import java.io.Closeable
import java.time.Duration
import java.time.Instant
import kotlin.coroutines.CoroutineContext
import kotlin.random.Random
import kotlin.time.Duration.Companion.seconds

private val LOGGER = LoggerFactory.getLogger(GarbageCollector::class.java)

class GarbageCollector @JvmOverloads constructor(
    db: Database,
    private val blocksToKeep: Int,
    private val garbageLifetime: Duration,
    private val approxRunInterval: Duration,
    private val coroutineCtx: CoroutineContext = Dispatchers.IO
) : Closeable {
    private val parentJob = Job()

    private val blockCatalog = db.blockCatalog
    private val trieCatalog = db.trieCatalog
    private val bufferPool = db.bufferPool

    private val blockGc = BlockGarbageCollector(blockCatalog, bufferPool, blocksToKeep)

    private fun defaultGarbageAsOf(): Instant? =
        blockCatalog.blockFromLatest(blocksToKeep)
            ?.let { it.latestCompletedTx.systemTime.microsAsInstant - garbageLifetime }

    @JvmOverloads
    fun garbageCollectTries(garbageAsOf: Instant? = defaultGarbageAsOf()) {
        if (garbageAsOf == null) return

        LOGGER.debug("Garbage collecting data older than {}", garbageAsOf)

        try {
            LOGGER.debug("Starting trie garbage collection")
            val tableNames = blockCatalog.allTables.shuffled().take(100)

            for (tableName in tableNames) {
                val garbageTries = trieCatalog.garbageTries(tableName, garbageAsOf)
                for (garbageTrie in garbageTries) {
                    bufferPool.deleteIfExists(tableName.metaFilePath(garbageTrie))
                    bufferPool.deleteIfExists(tableName.dataFilePath(garbageTrie))
                }
                trieCatalog.deleteTries(tableName, garbageTries)
            }
            LOGGER.debug("Trie garbage collection completed")

        } catch (e: Exception) {
            LOGGER.warn("Block garbage collection failed", e)
        } catch (e: Throwable) {
            throw RuntimeException("Error encountered during Block garbage collection: ", e)
        }
    }

    fun collectGarbage() {
        LOGGER.debug("Starting block garbage collection")
        blockGc.garbageCollectBlocks()
        LOGGER.debug("Block garbage collection completed")

        garbageCollectTries()
        LOGGER.debug("Next GC run scheduled in ${approxRunInterval.toMillis()}ms")
    }

    fun start() {
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
