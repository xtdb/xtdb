package xtdb.indexer

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.selectUnbiased
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.api.DatabaseName
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.error.Anomaly
import xtdb.api.error.Fault
import xtdb.api.log.DbOp
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.ReplicaMessage.BlockBoundary
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.api.tx.ExternalSource
import xtdb.database.Database
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.table.fromSchemaAndTable
import xtdb.types.MessageId
import xtdb.util.StringUtil.asLexHex
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.info
import xtdb.util.logger
import xtdb.util.useAll
import java.time.Duration
import java.time.InstantSource

/**
 * A higher-term record read back on our own replica log: a newer leader has superseded us. Thrown from
 * the apply loop to fail the term cleanly (not a query-facing fault, so it doesn't poison the watchers);
 * the transport re-follows on the next rebalance. See #5817.
 */
internal class LeaderSupersededException(message: String) : RuntimeException(message)

private val LOG = LeaderLogProcessor::class.logger

internal class LeaderLogProcessor(
    private val al: BufferAllocator,
    nodeBase: NodeBase,
    partitionStorage: PartitionStorage,
    crashLogger: CrashLogger,
    partitionState: PartitionState,
    private val dbName: DatabaseName,
    logsDriver: LogProcessor.LogsDriver,
    private val blockCutter: BlockCutter,
    private val watchers: Watchers,

    private val replicaAppender: ReplicaLogAppender,
    private val termFence: TermFence,

    extSource: ExternalSource?,
    skipTxs: Set<MessageId>,
    private val dbCatalog: Database.Catalog?,
    private val leaderTerm: Long = 0,
    instantSource: InstantSource = InstantSource.system(),
    flushTimeout: Duration,
    ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : AutoCloseable {

    init {
        check((dbCatalog != null) == (dbName == "xtdb")) { "dbCatalog must be provided iff database is 'xtdb'" }
    }

    private val partition = partitionStorage.partition

    private val bufferPool = partitionStorage.bufferPool
    private val tableCatalog = partitionState.tableCatalog
    private val trieCatalog = partitionState.trieCatalog
    private val liveIndex = partitionState.liveIndex

    // Resolves each source-log / attach-detach / ext-source tx and holds it — with every other
    // resolved-but-not-yet-applied tx — until we've read it back off our own replica log and committed it
    // into the live index. Driven only from the persister coroutine, and freed in close() once that job is
    // joined; see TxResolver.
    private val txResolver =
        TxResolver(
            al, nodeBase, partitionStorage, partitionState, dbName, crashLogger, skipTxs,
            resolvedSrcMsgId = watchers.latestSourceMsgId, resolvedExtToken = watchers.externalSourceToken,
            instantSource
        )

    val gc = GarbageCollector(
        nodeBase, partitionStorage, partitionState, dbName, leaderTerm, replicaAppender, ioDispatcher
    )

    val srcLogProc = SourceLogProcessor(
        partitionState, dbCatalog, dbName,
        leaderTerm, logsDriver, txResolver, blockCutter, replicaAppender, flushTimeout
    )

    val extSrcProc =
        extSource?.let { source ->
            ExternalSourceProcessor(
                source, tableCatalog, watchers, txResolver, blockCutter, replicaAppender,
                partition = partition, leaderTerm = leaderTerm
            )
        }

    /**
     * The block this term has produced and not yet read back, if any.
     *
     * A demotion hands it to the next follower — see [BlockCutter.pendingBlock] for what goes wrong
     * without that.
     */
    val pendingBlock get() = blockCutter.pendingBlock

    private fun applyResolvedTx(msg: ReplicaMessage.ResolvedTx) {
        val txKey = TransactionKey(msg.txId, msg.systemTime)

        msg.loadTableData(al).useAll { tables -> liveIndex.commitTx(txKey, tables) }

        val result =
            if (msg.committed) TransactionResult.Committed(txKey)
            else TransactionResult.Aborted(txKey, msg.error)

        // Handling for pre-`f3eb8d7d9` ResolvedTx records — see #5586.
        val effectiveSrcMsgId = msg.srcMsgId
            ?: if (extSrcProc != null) watchers.latestSourceMsgId else msg.txId

        watchers.notifyApplied(effectiveSrcMsgId, result, msg.externalSourceToken)
    }

    private fun applyResolvedTx(tx: ResolvedTx) {
        try {
            liveIndex.commitTx(tx.txKey, tx.allTables.associate { it.ref to it.relation })

            watchers.notifyApplied(tx.srcMsgId, tx.txResult, tx.externalSourceToken)

            tx.pending?.complete(tx.txResult)
        } catch (e: Throwable) {
            // tx is already off the queue, so teardown's failPending can't reach it — this catch is the
            // only thing that will ever fail its handle.
            tx.pending?.completeExceptionally(e)
            throw e
        }
    }

    private fun applyDbOp(dbOp: DbOp?) {
        // Only the primary carries a catalog, and a secondary's log can hold dbOps all the same: it was
        // some other cluster's primary before it was attached here, and those are that cluster's to apply.
        val dbCatalog = dbCatalog ?: return

        try {
            when (dbOp) {
                is DbOp.Attach -> dbCatalog.attach(dbOp.dbName, dbOp.config)
                is DbOp.Detach -> dbCatalog.detach(dbOp.dbName)
                null -> {}
            }
        } catch (e: Anomaly.Caller) {
            // A caller fault at resolution belongs to whoever submitted the attach, and aborts their
            // transaction. The same failure here belongs to nobody: the transaction has committed, no
            // caller is left to act on it, and nothing re-reads the instruction. So it is this node's
            // fault, and it stops the database rather than being reported.
            //
            // Every refusal reaching here says this node disagrees with the log — including one that
            // names a database it already holds, because holding the name says nothing about holding
            // it under the config the log just carried.
            //
            // Carrying on is the worse option, not the safer one: a block records the whole secondary
            // list and replaces the previous one, so the next boundary would erase a database this node
            // merely failed to open, for the entire cluster.
            throw Fault(
                "[$dbName] could not apply $dbOp", "xtdb/db-op-not-applied",
                mapOf("db-name" to dbName, "db-op" to dbOp.toString()), e
            )
        }
    }

    suspend fun applyReplicaMessage(record: Log.Record<ReplicaMessage>) {
        val msgTermId = record.message.termId

        if (msgTermId > leaderTerm)
            throw LeaderSupersededException("[$dbName] superseded: read term $msgTermId > our term $leaderTerm at ${record.msgId}")

        // Ahead of the fence, as the follower does: a record held behind an open block has not been acted
        // on, so folding it here would move the high-water past the term that cut the boundary — and the
        // upload closing the block, written by that same term, would then be fenced away.
        blockCutter.pendingBlock?.let { pending ->
            val msg = record.message

            if (msg is ReplicaMessage.BlockUploaded && blockCutter.closes(msg)) closeBlock(record, msg)
            else pending += record

            return
        }

        // Our own claim folded before this term opened, so the fence's high-water is
        // our term and anything it refuses is below ours — which shouldn't appear
        // past our replay target anyway. The fold has to happen here whatever the
        // verdict, or the high-water would stand still for the length of the term.
        if (!termFence.admit(msgTermId)) {
            LOG.debug { "[$dbName] leader: discarding fenced record ${record.msgId} (term $msgTermId < $leaderTerm)" }
        } else {
            when (val msg = record.message) {
                is ReplicaMessage.ResolvedTx -> {
                    // Ahead of the tx itself, so a caller that submitted an attach can use the database as
                    // soon as its transaction returns — and once, whichever way the tx below is applied.
                    // Nothing to guard on `committed`: a refused dbOp resolves to an abort carrying no dbOp.
                    applyDbOp(msg.dbOp)

                    txResolver.removeHead(msg.txId).use { tx ->
                        if (tx != null) {
                            applyResolvedTx(tx)
                        } else {
                            applyResolvedTx(msg)
                        }
                    }
                }

                is ReplicaMessage.TriesAdded -> {
                    if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch)
                        msg.tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
                            trieCatalog.addTries(fromSchemaAndTable(tableName), tries, record.logTimestamp)
                        }

                    // Below the guard, not above it: the compactor awaits this watermark and then
                    // recalculates jobs off the catalog, so notifying first would have it re-select the
                    // job it has just published.
                    watchers.notifyApplied(msg.sourceMsgId)
                }

                is BlockBoundary -> {
                    // Produce only: the catalog refresh, the index roll, the source watermark and the
                    // resolution resume all wait for the `BlockUploaded` this appends to come back — see
                    // [closeBlock].
                    blockCutter.upload(PendingBlock(record.msgId, msg))
                }

                // Two terms can produce one block index: a promoting follower still holding the boundary
                // produces that block itself, and `closes` matches on index, version and epoch rather
                // than on term, so a role adopts on whichever upload reaches it first and the loser
                // arrives here. Stale by the test every other reader applies — only an index the catalog
                // has not reached is left unexplained.
                is ReplicaMessage.BlockUploaded ->
                    if (msg.blockIndex > (tableCatalog.currentBlockIndex ?: -1))
                        error(
                            "[$dbName] BlockUploaded b${msg.blockIndex.asLexHex} at ${record.msgId} " +
                                    "with no block in flight"
                        )

                is ReplicaMessage.NoOp -> watchers.notifyApplied(msg.srcMsgId)

                is ReplicaMessage.TriesDeleted -> gc.triesDeleted(msg)
            }
        }
    }

    /**
     * Adopt the block our own upload confirms, then apply what its boundary was holding back.
     *
     * The drain goes back through [applyReplicaMessage] rather than applying the records directly, so a
     * boundary among them opens the next block there and the records behind it are held again instead of
     * being applied into a block already snapshotted.
     */
    private suspend fun closeBlock(
        record: Log.Record<ReplicaMessage>, msg: ReplicaMessage.BlockUploaded,
    ) {
        val pending = blockCutter.closeBlock(msg, record.logTimestamp)

        watchers.notifyApplied(msg.latestProcessedMsgId)

        gc.signal()

        srcLogProc.blockUploaded()

        pending.bufferedRecords.forEach { applyReplicaMessage(it) }
    }

    // ---- resolution ----

    /**
     * Run the term until it ends, then fail everything staged on it.
     *
     * A supersession is not a fault — it says this node is merely no longer the leader — so it MUST NOT
     * reach the watchers: `Failed` is absorbing, and poisoning them over a resignation would leave a
     * healthy database unqueryable until the process restarts (#5817).
     */
    suspend fun runTerm(replicaMsgs: ReceiveChannel<ReplicaApply>) {
        try {
            coroutineScope {
                launch { gc.runGc() }
                extSrcProc?.let { extSrcProc -> launch { extSrcProc.run() } }

                launch(CoroutineName("$dbName-replica-appender")) { replicaAppender.run() }

                while (true) {
                    // Ahead of the arming below, not after it: a filled block must admit nothing else, and
                    // the GC clause would otherwise slip a TriesDeleted in ahead of the boundary.
                    if (blockCutter.isFull)
                        blockCutter.cut(txResolver.resolvedSrcMsgId, txResolver.resolvedExtToken)

                    selectUnbiased {
                        replicaMsgs.onReceive { pending ->
                            try {
                                applyReplicaMessage(pending.record)
                                pending.applied.complete(Unit)
                            } catch (t: Throwable) {
                                pending.applied.completeExceptionally(t.asCancellation())
                                throw t
                            }
                        }

                        if (blockCutter.acceptingResolution) {
                            srcLogProc.run { armSelect() }
                            extSrcProc?.run { armSelect() }
                            gc.run { armSelect() }
                        }
                    }
                }
            }

        } catch (t: Throwable) {
            when {
                t is LeaderSupersededException -> {
                    LOG.info("[$dbName] ${t.message}")
                }

                !t.isShutdownSignal -> {
                    LOG.error(t) { "[$dbName] leader term failed" }
                    watchers.notifyError(t)
                }
            }

            // A flat sweep rather than per-caller handling: nothing may be left awaiting a term that has gone,
            // and the symptom of missing one is a hang, not an error (#5711 / #5817).
            txResolver.failPending(t)
            srcLogProc.shutdown(t)
            extSrcProc?.shutdown(t)
            gc.shutdown(t)
            replicaAppender.shutdown(t)
        }
    }

    override fun close() {
        txResolver.close()
    }
}
