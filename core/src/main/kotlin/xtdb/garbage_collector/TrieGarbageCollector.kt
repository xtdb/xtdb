package xtdb.garbage_collector

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.CONFLATED
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.selects.select
import xtdb.catalog.BlockCatalog.Companion.blockFromLatest
import xtdb.database.DatabaseState
import xtdb.storage.BufferPool
import xtdb.table.TableRef
import xtdb.time.microsAsInstant
import xtdb.trie.Trie.dataFilePath
import xtdb.trie.Trie.metaFilePath
import xtdb.trie.TrieKey
import xtdb.util.debug
import xtdb.util.logger
import xtdb.util.warn
import java.time.Duration
import java.time.Instant
import kotlin.time.Duration.Companion.seconds

private val LOGGER = TrieGarbageCollector::class.logger

private const val DEFAULT_TABLE_PARALLELISM = 8
private const val DEFAULT_DELETE_PARALLELISM = 64

// Caps the number of trie keys per `TriesDeleted` message. Months of accumulated backlog on a
// hot table can produce tens of thousands of garbage trie keys; a single message would blow
// past Kafka's `max.message.bytes` (1 MB by default).
private const val TRIES_DELETED_CHUNK_SIZE = 1024

/**
 * Leader-owned cleanup of stale trie files (meta + data).
 *
 * The leader [signal]s at every block boundary; followers never construct one. Fire-and-forget:
 * tx processing never waits for a GC cycle (GC has been off for months on existing deployments —
 * the first cycle's backlog could stall the indexer for minutes if it were awaited, so the
 * leader's block-boundary path uses non-suspending [signal] instead — see PR #5511).
 *
 * Per-table ordering: obj-store DELETE → atomic ([commitTriesDeleted]) publish-and-commit.
 * A crash mid-cycle leaves orphaned catalog entries that the next cycle re-DELETEs idempotently
 * (S3 returns 404, fine); the reverse order would leave followers thinking deleted files were
 * still live, which is unsafe.
 *
 * Parallelism is bounded in two dimensions: [tableParallelism] tables in flight concurrently,
 * [deleteParallelism] DELETEs in flight across the whole cycle (shared pool).
 */
@OptIn(ExperimentalCoroutinesApi::class)
class TrieGarbageCollector(
    private val bufferPool: BufferPool,
    dbState: DatabaseState,
    /**
     * Publishes a `TriesDeleted` to the replica log AND removes the keys from the local trie
     * catalog — atomically under the leader's replica-log mutex.
     * The two writes MUST happen under the same lock: see the call site for the block-file
     * consistency rationale.
     */
    private val commitTriesDeleted: suspend (tableName: TableRef, trieKeys: Set<TrieKey>) -> Unit,
    private val blocksToKeep: Int,
    private val garbageLifetime: Duration,
    /** Gates [signal]s from the leader's block-boundary path; direct [awaitNoGarbage] is unaffected. */
    val enabled: Boolean,
    private val meterRegistry: MeterRegistry? = null,
    tableParallelism: Int = DEFAULT_TABLE_PARALLELISM,
    deleteParallelism: Int = DEFAULT_DELETE_PARALLELISM,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
) : AutoCloseable {

    private val blockCatalog = dbState.blockCatalog
    private val trieCatalog = dbState.trieCatalog

    private val dbJob = Job()
    private val scope = CoroutineScope(dispatcher + dbJob)

    // [signal] is fire-and-forget; bursts coalesce into one upcoming cycle.
    private val signalCh = Channel<Unit>(CONFLATED)
    private val awaitCh = Channel<CompletableDeferred<Unit>>(UNLIMITED, onUndeliveredElement = { it.cancel() })

    private val tableDispatcher = dispatcher.limitedParallelism(tableParallelism)
    private val deleteDispatcher = dispatcher.limitedParallelism(deleteParallelism)

    private val deleteTimer: Timer? = meterRegistry?.let {
        Timer.builder("xtdb.gc.tries.delete.timer")
            .publishPercentiles(0.75, 0.95, 0.99)
            .register(it)
    }
    
    init {
        LOGGER.debug("Starting TrieGarbageCollector (enabled=$enabled, blocksToKeep=$blocksToKeep, garbageLifetime=$garbageLifetime)")

        scope.launch {
            while (isActive) {
                val pending = mutableListOf<CompletableDeferred<Unit>>()

                select<Unit> {
                    if (enabled) signalCh.onReceive { }
                    awaitCh.onReceive { pending += it }
                }

                try {
                    do {
                        signalCh.tryReceive()
                        while (true) pending.add(awaitCh.tryReceive().getOrNull() ?: break)
                        garbageCollectTries()
                    } while (drainTriggers(pending))

                    pending.forEach { it.complete(Unit) }
                } catch (e: CancellationException) {
                    pending.forEach { it.cancel() }
                    throw e
                } catch (e: Exception) {
                    LOGGER.warn(e, "Trie garbage collection cycle failed")
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

    private fun defaultGarbageAsOf(): Instant? =
        bufferPool.blockFromLatest(blocksToKeep)
            ?.let { it.latestCompletedTx.systemTime.microsAsInstant - garbageLifetime }

    suspend fun garbageCollectTries(garbageAsOf: Instant? = null) {
        val asOf = garbageAsOf ?: defaultGarbageAsOf() ?: return

        LOGGER.debug("Garbage collecting tries older than $asOf")

        supervisorScope {
            for (tableName in blockCatalog.allTables) {
                launch(tableDispatcher) {
                    try {
                        garbageCollectTable(tableName, asOf)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        LOGGER.warn(e, "Trie GC failed for table $tableName")
                    }
                }
            }
        }
    }

    private suspend fun garbageCollectTable(tableName: TableRef, asOf: Instant) {
        val garbageTries = trieCatalog.garbageTries(tableName, asOf)
        if (garbageTries.isEmpty()) return

        // Chunked so a single `TriesDeleted` stays under Kafka's max-message-bytes; per-table
        // DELETE-then-commit ordering is preserved within each chunk. A crash between chunks
        // leaves the un-committed chunk's files deleted but still in the catalog — next cycle
        // re-DELETEs (S3 is idempotent) and re-publishes, so this is recoverable.
        for (chunk in garbageTries.chunked(TRIES_DELETED_CHUNK_SIZE)) {
            // Any DELETE failure rethrows out of `coroutineScope` and aborts this chunk before
            // publishing — followers must never drop catalog entries for files still on disk.
            // Data file before meta because meta is the "completed file" marker for a trie pair;
            // deleting it last keeps the pair from ever transiently looking complete-but-empty.
            coroutineScope {
                for (trieKey in chunk) {
                    launch(deleteDispatcher) {
                        val timer = meterRegistry?.let { Timer.start(it) }
                        bufferPool.deleteIfExists(tableName.dataFilePath(trieKey))
                        bufferPool.deleteIfExists(tableName.metaFilePath(trieKey))
                        deleteTimer?.let { timer?.stop(it) }
                    }
                }
            }

            commitTriesDeleted(tableName, chunk.toSet())
        }
    }

    /**
     * Schedule a cycle, fire-and-forget. Bursts coalesce into a single upcoming cycle. Used by
     * the leader's block-boundary path so tx processing never blocks on GC progress.
     */
    fun signal() {
        signalCh.trySend(Unit)
    }

    /**
     * Suspend until a cycle that started at or after this call has completed — every waiter sees
     * a cycle whose start post-dated their arrival, even if another waiter joined mid-cycle.
     * Intended for tests and admin pokes; production triggers via [signal].
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
                ?: LOGGER.warn("Trie GC coroutine did not stop within 5s")
        }
    }
}
