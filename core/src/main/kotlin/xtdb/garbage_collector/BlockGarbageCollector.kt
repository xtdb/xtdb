package xtdb.garbage_collector

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.CONFLATED
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.selects.select
import xtdb.catalog.BlockCatalog
import xtdb.catalog.BlockCatalog.Companion.allBlockFiles
import xtdb.catalog.BlockCatalog.Companion.tableBlocks
import xtdb.storage.BufferPool
import xtdb.util.StringUtil.fromLexHex
import xtdb.util.debug
import xtdb.util.logger
import xtdb.util.warn
import java.nio.file.Path
import kotlin.time.Duration.Companion.seconds

private val LOGGER = BlockGarbageCollector::class.logger

private const val DEFAULT_TABLE_PARALLELISM = 8
private const val DEFAULT_DELETE_PARALLELISM = 64

/**
 * Leader-owned cleanup of stale block / table-block files.
 *
 * The leader [signal]s at every block boundary; followers never construct one. Block garbage
 * only appears as a side-effect of block uploads, so this is the natural trigger — no timer
 * needed. Fire-and-forget: tx processing never waits for a GC cycle to complete (GC has been
 * disabled for months, so the first cycle's backlog could be huge — see PR #5511).
 *
 * Parallelism is bounded in two dimensions: [tableParallelism] tables concurrently,
 * [deleteParallelism] DELETEs in flight across the whole cycle (shared pool).
 */
@OptIn(ExperimentalCoroutinesApi::class)
class BlockGarbageCollector(
    private val bufferPool: BufferPool,
    private val blockCatalog: BlockCatalog,
    private val blocksToKeep: Int,
    /** Gates the auto-signal from the leader's block-boundary path; direct `awaitNoGarbage()` is unaffected. */
    val enabled: Boolean,
    private val meterRegistry: MeterRegistry? = null,
    tableParallelism: Int = DEFAULT_TABLE_PARALLELISM,
    deleteParallelism: Int = DEFAULT_DELETE_PARALLELISM,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
) : AutoCloseable {

    private val dbJob = Job()
    private val scope = CoroutineScope(dispatcher + dbJob)

    // [signal] is fire-and-forget; bursts coalesce into one upcoming cycle.
    private val signalCh = Channel<Unit>(CONFLATED)

    // [awaitNoGarbage] callers each get a `CompletableDeferred` resumed after the next cycle —
    // every waiter must be preserved.
    private val awaitCh = Channel<CompletableDeferred<Unit>>(UNLIMITED, onUndeliveredElement = { it.cancel() })

    private val tableDispatcher = dispatcher.limitedParallelism(tableParallelism)
    private val deleteDispatcher = dispatcher.limitedParallelism(deleteParallelism)

    private val blockDeleteTimer: Timer? = meterRegistry?.let {
        Timer.builder("xtdb.gc.block_files.delete.timer")
            .publishPercentiles(0.75, 0.95, 0.99)
            .register(it)
    }

    private val tableBlockDeleteTimer: Timer? = meterRegistry?.let {
        Timer.builder("xtdb.gc.table_block_files.delete.timer")
            .publishPercentiles(0.75, 0.95, 0.99)
            .register(it)
    }

    init {
        require(blocksToKeep >= 1) { "blocksToKeep must be >= 1, got $blocksToKeep" }

        LOGGER.debug("Starting BlockGarbageCollector (enabled=$enabled, blocksToKeep=$blocksToKeep)")

        scope.launch {
            while (isActive) {
                val pending = mutableListOf<CompletableDeferred<Unit>>()

                // Wait for any trigger. Drain both channels each cycle so waiters arriving while
                // a cycle is running see a cycle that *started* after their suspension — not just
                // one that finished after it.
                select {
                    if (enabled) signalCh.onReceive { }
                    awaitCh.onReceive { pending += it }
                }

                try {
                    do {
                        signalCh.tryReceive()
                        while (true) pending.add(awaitCh.tryReceive().getOrNull() ?: break)
                        garbageCollectBlocks()
                    } while (drainTriggers(pending))

                    pending.forEach { it.complete(Unit) }
                } catch (e: CancellationException) {
                    pending.forEach { it.cancel() }
                    throw e
                } catch (e: Exception) {
                    LOGGER.warn(e, "Block garbage collection cycle failed")
                    pending.forEach { it.completeExceptionally(e) }
                }
            }
        }
    }

    private fun drainTriggers(pending: MutableList<CompletableDeferred<Unit>>): Boolean {
        var any = signalCh.tryReceive().isSuccess
        while (true) {
            val w = awaitCh.tryReceive().getOrNull() ?: break
            pending.add(w)
            any = true
        }
        return any
    }

    private fun Path.parseBlockIndex(): Long? =
        Regex("b(\\p{XDigit}+)\\.binpb")
            .matchEntire(fileName.toString())
            ?.groups?.get(1)
            ?.value?.fromLexHex

    suspend fun garbageCollectBlocks() {
        LOGGER.debug("Garbage collecting blocks, keeping $blocksToKeep blocks")

        val latestBlockIndex = blockCatalog.currentBlockIndex ?: return

        fun Path.isGarbage(): Boolean =
            parseBlockIndex()?.let { it != latestBlockIndex && it <= latestBlockIndex - blocksToKeep } ?: false

        suspend fun deleteGarbage(paths: Sequence<Path>, gcTimer: Timer? = null) {
            coroutineScope {
                paths.filter { it.isGarbage() }.forEach { path ->
                    launch(deleteDispatcher) {
                        val timer = meterRegistry?.let { Timer.start(it) }
                        bufferPool.deleteIfExists(path)
                        gcTimer?.let { timer?.stop(it) }
                    }
                }
            }
        }

        supervisorScope {
            launch(tableDispatcher) {
                deleteGarbage(bufferPool.allBlockFiles.asSequence().map { it.key }, blockDeleteTimer)
            }
        }

        supervisorScope {
            for (table in blockCatalog.allTables) {
                launch(tableDispatcher) {
                    deleteGarbage(bufferPool.tableBlocks(table).asSequence().map { it.key }, tableBlockDeleteTimer)
                }
            }
        }
    }

    /**
     * Schedule a cycle, fire-and-forget. Bursts coalesce into a single upcoming cycle (the
     * underlying signal channel is conflated). Used by the leader's block-boundary path so tx
     * processing never blocks on GC progress.
     */
    fun signal() {
        signalCh.trySend(Unit)
    }

    /**
     * Suspend until a cycle that started at or after this call has run to completion — every
     * waiter sees a cycle whose start post-dated their arrival, even if another waiter joined
     * mid-cycle. Intended for tests and admin pokes; production triggers via [signal].
     */
    suspend fun awaitNoGarbage() {
        val deferred = CompletableDeferred<Unit>()
        awaitCh.send(deferred)
        try {
            deferred.await()
        } catch (e: CancellationException) {
            // Caller-side cancellation: tell the loop not to bother running a cycle on our behalf.
            // Idempotent if the deferred was already cancelled (loop shutdown).
            deferred.cancel(e)
            throw e
        }
    }

    fun awaitNoGarbageBlocking() = runBlocking { awaitNoGarbage() }

    override fun close() {
        runBlocking {
            withTimeoutOrNull(5.seconds) { dbJob.cancelAndJoin() }
                ?: LOGGER.warn("Block GC coroutine did not stop within 5s")
        }
    }
}
