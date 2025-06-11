package xtdb.garbage_collector

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import xtdb.catalog.BlockCatalog
import xtdb.time.microsAsInstant
import xtdb.trie.TrieCatalog
import java.io.Closeable
import java.time.Duration
import java.time.Instant
import kotlin.random.Random
import kotlin.time.Duration.Companion.seconds

private val LOGGER = LoggerFactory.getLogger(GarbageCollector::class.java)


class GarbageCollector(
    private val enabled : Boolean,
    private val blockCatalog: BlockCatalog,
    private val blocksToKeep: Int,
    private val trieCatalog: TrieCatalog,
    private val garbageLifetime: Duration,
    private val approxRunInterval: Duration,
) : Closeable {
    private val scope = CoroutineScope(Dispatchers.IO)

    // explicit function for testing
    fun garbageCollect(garbageAsOf: Instant?) {
        try {
            LOGGER.debug("Starting block garbage collection")
            blockCatalog.garbageCollectBlocks(blocksToKeep)
            LOGGER.debug("Block garbage collection completed")

            if (garbageAsOf != null) {
                LOGGER.debug("Starting trie garbage collection")
                val tableNames = blockCatalog.allTableNames.take(100)
                for (tableName in tableNames) {
                    trieCatalog.garbageCollectTries(tableName, garbageAsOf)
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

    init {
        LOGGER.info("Starting GarbageCollector with approxRunInterval: $approxRunInterval, blocksToKeep: $blocksToKeep")
        if (enabled) {
            scope.launch {
                delay(Random.nextLong(approxRunInterval.toMillis()))
                while (isActive) {
                    garbageCollectFromOldestToKeep()
                    LOGGER.debug("Next GC run scheduled in ${approxRunInterval.toMillis()}ms")
                    delay(approxRunInterval.toMillis())
                }
            }
        }
    }

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
        LOGGER.info("GarbageCollector shut down")
    }
}
