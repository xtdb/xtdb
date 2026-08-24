package xtdb.indexer

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import xtdb.NodeBase
import xtdb.api.DatabaseName
import xtdb.api.TableRef
import xtdb.api.log.ReplicaMessage.TriesDeleted
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.garbage_collector.BlockGarbageCollector
import xtdb.garbage_collector.TrieGarbageCollector
import xtdb.trie.TrieKey

internal class GarbageCollector(
    private val nodeBase: NodeBase,
    partitionStorage: PartitionStorage,
    private val partitionState: PartitionState,
    private val dbName: DatabaseName,
    private val leaderTerm: Long,
    private val replicaAppender: ReplicaLogAppender,

    // Base for the GCs' delete fan-out; defaults to IO in prod, sims inject the seeded dispatcher.
    gcDispatcher: CoroutineDispatcher = Dispatchers.IO,
) {

    private val bufferPool = partitionStorage.bufferPool
    private val tableCatalog = partitionState.tableCatalog
    private val trieCatalog = partitionState.trieCatalog

    private val blockGc = nodeBase.config.garbageCollector.let { cfg ->
        BlockGarbageCollector(
            bufferPool, tableCatalog,
            blocksToKeep = cfg.blocksToKeep,
            enabled = cfg.enabled,
            meterRegistry = nodeBase.meterRegistry,
            dispatcher = gcDispatcher,
            dbName = dbName
        )
    }

    private val gcCh = Channel<GcTask>(
        Channel.UNLIMITED,
        onUndeliveredElement = { it.abandon(CancellationException("leader term closed")) }
    )

    val onTask get() = gcCh.onReceive

    private val trieGc = nodeBase.config.garbageCollector.let { cfg ->
        // Routed through the persister rather than applied inline: the catalog removal has to be serialised
        // against block cuts, and this await must not return until the catalog reflects it — which is the
        // GC's contract, since it has already deleted the files. See [handleTask].
        val commitTriesDeleted: suspend (TableRef, Set<TrieKey>) -> Unit = { tableName, trieKeys ->
            val task = GcTask.TriesDeleted(tableName, trieKeys)
            gcCh.send(task)
            task.onComplete.await()
        }

        TrieGarbageCollector(
            bufferPool, partitionState, dbName,
            commitTriesDeleted, cfg.blocksToKeep, cfg.garbageLifetime,
            cfg.enabled,
            nodeBase.meterRegistry,
            dispatcher = gcDispatcher,
        )
    }

    sealed class GcTask {
        val onComplete = CompletableDeferred<Unit>()

        fun abandon(cause: Throwable) {
            onComplete.completeExceptionally(cause)
        }

        data class TriesDeleted(val tableName: TableRef, val trieKeys: Set<TrieKey>) : GcTask()
    }

    suspend fun handleTask(task: GcTask) {
        when (task) {
            is GcTask.TriesDeleted -> {
                // Remove from the local catalog here, then replicate for followers — eager on the resolve side so
                // the GC's `commitTriesDeleted` await returns with the catalog already updated (its contract; the
                // GC has already deleted the files). Safe as a fenced-log projection for the same reason as
                // TriesAdded: the block-cut pause serialises this against any boundary, and gcCh is excluded while a
                // block is in progress. Skipped on our own consume-back (see applyRecord); the follower applies it.
                trieCatalog.deleteTries(task.tableName, task.trieKeys)

                replicaAppender.append(
                    ControlItem(TriesDeleted(task.tableName.schemaAndTable, task.trieKeys, termId = leaderTerm))
                )

                task.onComplete.complete(Unit)
            }
        }
    }

    fun signal() {
        blockGc.signal()
        trieGc.signal()
    }

    suspend fun runGc() = supervisorScope {
        launch { blockGc.run() }
        launch { trieGc.run() }
    }

    fun awaitNoGarbageBlocking() {
        blockGc.awaitNoGarbageBlocking()
        trieGc.awaitNoGarbageBlocking()
    }

    fun shutdown(cause: Throwable) {
        gcCh.close(cause)
        while (true) (gcCh.tryReceive().getOrNull() ?: break).abandon(cause)
    }
}