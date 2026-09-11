package xtdb.indexer

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.SelectBuilder
import kotlinx.coroutines.supervisorScope
import xtdb.NodeBase
import xtdb.api.DatabaseName
import xtdb.api.TableRef
import xtdb.api.log.ReplicaMessage.TriesDeleted
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.garbage_collector.BlockGarbageCollector
import xtdb.garbage_collector.TrieGarbageCollector
import xtdb.table.fromSchemaAndTable
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

    // Tasks whose `TriesDeleted` is appended and not read back yet. Positional: the log hands them back in
    // append order, so the head is the one that the record arriving confirms.
    //
    // A transient — the term's coroutine is the only thing that touches it, across the arm below,
    // [triesDeleted] and [shutdown].
    private val inFlight = ArrayDeque<GcTask>()

    fun SelectBuilder<Unit>.armSelect() {
        gcCh.onReceive { task ->
            try {
                when (task) {
                    is GcTask.TriesDeleted -> {
                        replicaAppender.append(
                            ControlItem(TriesDeleted(task.tableName.schemaAndTable, task.trieKeys, termId = leaderTerm))
                        )

                        inFlight += task
                    }
                }
            } catch (e: Throwable) {
                task.onComplete.completeExceptionally(e)
                throw e
            }
        }
    }

    /**
     * Remove [trieKeys] from the trie catalog and tell the rest of the cluster, returning once this node
     * has applied it.
     *
     * Handed to the persister and awaited rather than applied here: the GC has already deleted the files,
     * so it must not carry on against a catalog that still lists them. The await ends at the read-back
     * rather than at the append, which is what leaves log order alone to keep the removal ahead of any
     * block upload appended behind it — see gc.allium's DualWriteOrdering.
     */
    suspend fun commitTriesDeleted(tableName: TableRef, trieKeys: Set<TrieKey>) {
        val task = GcTask.TriesDeleted(tableName, trieKeys)
        gcCh.send(task)
        task.onComplete.await()
    }

    fun triesDeleted(msg: TriesDeleted) {
        val task = inFlight.removeFirstOrNull()
            ?: error("[$dbName] TriesDeleted for '${msg.tableName}' read back with nothing in flight")

        // From the record, not from `task`, so this node applies exactly what every other node applies.
        trieCatalog.deleteTries(fromSchemaAndTable(msg.tableName), msg.trieKeys)

        task.onComplete.complete(Unit)
    }

    private val trieGc = nodeBase.config.garbageCollector.let { cfg ->
        TrieGarbageCollector(
            bufferPool, partitionState, dbName,
            ::commitTriesDeleted, cfg.blocksToKeep, cfg.garbageLifetime,
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

        inFlight.forEach { it.abandon(cause) }
        inFlight.clear()
    }
}