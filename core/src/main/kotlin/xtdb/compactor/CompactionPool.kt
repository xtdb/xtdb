package xtdb.compactor

import kotlinx.coroutines.*
import xtdb.util.logger
import xtdb.util.warn
import kotlin.time.Duration.Companion.seconds

private val LOGGER = CompactionPool::class.logger

class CompactionPool(threadCount: Int) : AutoCloseable {

    internal val scope = CoroutineScope(Dispatchers.Default)

    internal val jobsScope =
        CoroutineScope(
            Dispatchers.Default.limitedParallelism(threadCount, "compactor")
                    + SupervisorJob(scope.coroutineContext.job)
        )

    override fun close() {
        runBlocking {
            withTimeoutOrNull(10.seconds) { scope.coroutineContext.job.cancelAndJoin() }
                ?: LOGGER.warn("failed to close compactor cleanly in 10s")
        }
    }
}