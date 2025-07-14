package xtdb.garbage_collector

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import xtdb.database.Database
import xtdb.time.microsAsInstant
import xtdb.trie.Trie.dataFilePath
import xtdb.trie.Trie.metaFilePath
import java.io.Closeable
import java.time.Duration
import java.time.Instant
import kotlin.random.Random
import kotlin.time.Duration.Companion.seconds

private val LOGGER = LoggerFactory.getLogger(GarbageCollector::class.java)

class GarbageCollector(
    db: Database,
    private val blocksToKeep: Int,
    private val garbageLifetime: Duration,
    private val approxRunInterval: Duration,
) : Closeable {
    private val scope = CoroutineScope(Dispatchers.IO)

    private val blockCatalog = db.blockCatalog
    private val trieCatalog = db.trieCatalog
    private val bufferPool = db.bufferPool

    // explicit function for testing
    fun garbageCollect(garbageAsOf: Instant?) {
        try {
            LOGGER.debug("Starting block garbage collection")
            blockCatalog.garbageCollectBlocks(blocksToKeep)
            LOGGER.debug("Block garbage collection completed")

            if (garbageAsOf != null) {
                LOGGER.debug("Starting trie garbage collection")
                val tableNames = blockCatalog.allTables.take(100)
                for (tableName in tableNames) {
                    val garbageTries = trieCatalog.garbageTries(tableName, garbageAsOf)
                    for (garbageTrie in garbageTries) {
                        bufferPool.deleteIfExists(tableName.metaFilePath(garbageTrie))
                        bufferPool.deleteIfExists(tableName.dataFilePath(garbageTrie))
                    }
                    trieCatalog.deleteTries(tableName, garbageTries)
                }
                LOGGER.debug("Trie garbage collection completed")
            }

        } catch (e: Exception) {
            LOGGER.warn("Block garbage collection failed", e)
        } catch (e: Throwable) {
            throw RuntimeException("Error encountered during Block garbage collection: ", e)
        }
    }

    fun garbageCollectFromOldestToKeep() {
        val oldestBlockToKeep = blockCatalog.blockFromLatest(blocksToKeep)
        val gcCutOff = oldestBlockToKeep?.let { it.latestCompletedTx.systemTime.microsAsInstant - garbageLifetime }
        LOGGER.debug("Garbage collecting data older than {}", gcCutOff)
        garbageCollect(gcCutOff)
    }

    fun start() {
        LOGGER.debug(
            "Starting GarbageCollector with approxRunInterval: {}, blocksToKeep: {}",
            approxRunInterval, blocksToKeep
        )

        scope.launch {
            delay(Random.nextLong(approxRunInterval.toMillis()))
            while (isActive) {
                garbageCollectFromOldestToKeep()
                LOGGER.debug("Next GC run scheduled in ${approxRunInterval.toMillis()}ms")
                delay(approxRunInterval.toMillis())
            }
        }
    }

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
        LOGGER.debug("GarbageCollector shut down")
    }
}
