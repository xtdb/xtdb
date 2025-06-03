package xtdb.garbage_collector

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import xtdb.BufferPool
import xtdb.api.storage.ObjectStore
import xtdb.catalog.BlockCatalog
import xtdb.util.asPath
import java.io.Closeable
import java.time.Duration
import kotlin.random.Random
import kotlin.time.Duration.Companion.seconds

private val LOGGER = LoggerFactory.getLogger(GarbageCollector::class.java)

class GarbageCollector(
    private val blockCatalog: BlockCatalog,
    private val blocksToKeep: Int,
    private val gracePeriod: Duration,
    private val approxRunInterval: Duration
) : Closeable {
    private val scope = CoroutineScope(Dispatchers.IO)

    init {
        LOGGER.info("Starting GarbageCollector with approxRunInterval: $approxRunInterval, blocksToKeep: $blocksToKeep")
        scope.launch {
            delay(Random.nextLong(approxRunInterval.toMillis()))
            while (isActive) {
                try {
                    LOGGER.debug("Starting block garbage collection")
                    blockCatalog.garbageCollectBlocks(blocksToKeep, gracePeriod)
                    LOGGER.debug("Block garbage collection completed")
                } catch (e: Exception) {
                    LOGGER.warn("Block garbage collection failed", e)
                } catch (e: Throwable) {
                    throw RuntimeException("Error encountered during Block garbage collection: ", e)
                }

                LOGGER.debug("Next GC run scheduled in ${approxRunInterval.toMillis()}ms")
                delay(approxRunInterval.toMillis())
            }
        }
    }

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
        LOGGER.info("GarbageCollector shut down")
    }
}
