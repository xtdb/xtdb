package xtdb.garbage_collector

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.CONFLATED
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.selects.select
import xtdb.catalog.TableCatalog.Companion.blockFromLatest
import xtdb.api.DatabaseName
import xtdb.database.PartitionState
import xtdb.storage.BufferPool
import xtdb.api.TableRef
import xtdb.table.TableEntry
import xtdb.time.microsAsInstant
import xtdb.trie.Trie
import xtdb.trie.TrieKey
import xtdb.util.debug
import xtdb.util.logger
import xtdb.util.warn
import java.time.Duration
import java.time.Instant

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
 * tx processing never waits for a GC cycle — on a deployment that hasn't collected in a while, the
 * first cycle's backlog could stall the indexer for minutes if it were awaited, so the leader's
 * block-boundary path uses the non-suspending [signal] instead.
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
    partitionState: PartitionState,
    dbName: DatabaseName,
    /**
     * Publishes a `TriesDeleted` to the replica log AND removes the keys from the local trie catalog — atomically, in a single Persister task on the leader.
     * The Persister channel is the sole ordering point, so no other replica-log write interleaves between the two.
     * See the call site for the block-file consistency rationale.
     */
    private val commitTriesDeleted: suspend (tableName: TableRef, trieKeys: Set<TrieKey>) -> Unit,
    private val blocksToKeep: Int,
    private val garbageLifetime: Duration,
    val enabled: Boolean,
    private val meterRegistry: MeterRegistry? = null,
    tableParallelism: Int = DEFAULT_TABLE_PARALLELISM,
    deleteParallelism: Int = DEFAULT_DELETE_PARALLELISM,
    /** Base for the parallel delete fan-out pools; the loop itself runs on its caller's thread. Sims inject the seeded dispatcher so deletes stay on the simulation's thread. */
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
) {

    private val tableCatalog = partitionState.tableCatalog
    private val trieCatalog = partitionState.trieCatalog

    // [signal] is fire-and-forget; bursts coalesce into one upcoming cycle.
    private val signalCh = Channel<Unit>(CONFLATED)
    private val awaitCh = Channel<CompletableDeferred<Unit>>(UNLIMITED, onUndeliveredElement = { it.cancel() })

    private val tableDispatcher = dispatcher.limitedParallelism(tableParallelism)
    private val deleteDispatcher = dispatcher.limitedParallelism(deleteParallelism)

    private val deleteTimer: Timer? = meterRegistry?.let {
        Timer.builder("xtdb.gc.tries.delete.timer")
            .publishPercentiles(0.75, 0.95, 0.99)
            .tag("db", dbName)
            .register(it)
    }
    
    /** Collect on every trigger until cancelled. Nothing is serviced until this is running: [signal] queues, and [awaitNoGarbage] suspends. */
    suspend fun run(): Unit = coroutineScope {
        LOGGER.debug("Starting TrieGarbageCollector (enabled=$enabled, blocksToKeep=$blocksToKeep, garbageLifetime=$garbageLifetime)")

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
            for (entry in tableCatalog.snap().entries) {
                launch(tableDispatcher) {
                    try {
                        garbageCollectTable(entry, asOf)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        LOGGER.warn(e, "Trie GC failed for table ${entry.table}")
                    }
                }
            }
        }
    }

    private suspend fun garbageCollectTable(entry: TableEntry, asOf: Instant) {
        val tableName = entry.table
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
                    // Defence in depth: trieCatalog.garbageTries already filters L0 out — L0
                    // files are the recovery substrate for `reset-compactor!` and must never
                    // be deleted by GC. If anything ever flips that filter, we want to fail
                    // loud here rather than silently nuke the recovery path.
                    check(Trie.parseKey(trieKey).level != 0L) {
                        "L0 trie keys must never reach GC deletion: $trieKey"
                    }
                    launch(deleteDispatcher) {
                        val timer = meterRegistry?.let { Timer.start(it) }
                        bufferPool.deleteIfExists(entry.slug.dataFilePath(trieKey))
                        bufferPool.deleteIfExists(entry.slug.metaFilePath(trieKey))
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
}
