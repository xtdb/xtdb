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
import xtdb.trie.addTries
import xtdb.types.MessageId
import xtdb.util.StringUtil.asLexHex
import xtdb.util.closeAllOnCatch
import xtdb.util.useAll
import java.time.Duration
import java.time.InstantSource

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

    extSource: ExternalSource?,
    skipTxs: Set<MessageId>,
    private val dbCatalog: Database.Catalog?,
    val leaderTerm: Long = 0,
    instantSource: InstantSource = InstantSource.system(),
    flushTimeout: Duration,
    ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : AutoCloseable {

    init {
        check((dbCatalog != null) == (dbName == "xtdb")) { "dbCatalog must be provided iff database is 'xtdb'" }
    }

    private val partition = partitionStorage.partition
    private val sourceLog = partitionStorage.sourceLog

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

    private fun readBackMismatch(
        record: Log.Record<ReplicaMessage>, msg: ReplicaMessage.ResolvedTx, resolvedTxId: MessageId?,
    ) = Fault(
        "[$dbName] read back tx ${msg.txId} at ${record.msgId}, but the next tx this term resolved is ${resolvedTxId ?: "none"}",
        "xtdb.indexer/leader-read-back-mismatch",
        mapOf("replica-msg-id" to record.msgId, "tx-id" to msg.txId, "resolved-tx-id" to resolvedTxId),
    )

    private fun applyResolvedTx(tx: ResolvedTx) {
        try {
            tx.sealTables().closeAllOnCatch { liveIndex.applyTx(tx.txKey, it) }

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

        when (dbOp) {
            is DbOp.Attach -> dbCatalog.attach(dbOp.dbName, dbOp.config)
            is DbOp.Detach -> dbCatalog.detach(dbOp.dbName)
            null -> {}
        }
    }

    suspend fun applyReplicaMessage(record: Log.Record<ReplicaMessage>) {
        blockCutter.pendingBlock?.let { pending ->
            val msg = record.message

            if (msg is ReplicaMessage.BlockUploaded && blockCutter.closes(msg)) {
                val held = blockCutter.closeBlock(msg, record.logTimestamp)
                watchers.notifyApplied(msg.latestProcessedMsgId)
                gc.signal()
                srcLogProc.blockUploaded()
                held.bufferedRecords.forEach { applyReplicaMessage(it) }
            } else {
                pending += record
            }

            return
        }

        when (val msg = record.message) {
            is ReplicaMessage.ResolvedTx -> {
                // Ahead of the tx itself, so a caller that submitted an attach can use the database as
                // soon as its transaction returns — and once, whichever way the tx below is applied.
                // Nothing to guard on `committed`: a refused dbOp resolves to an abort carrying no dbOp.
                applyDbOp(msg.dbOp)

                // Below this term: a previous leader's, replayed from the block this term inherited, so never resolved here.
                if (msg.termId < leaderTerm) {
                    applyResolvedTx(msg)
                } else {
                    val head = txResolver.removeHead()
                        ?: throw readBackMismatch(record, msg, resolvedTxId = null)

                    head.use { tx ->
                        if (tx.txKey.txId != msg.txId)
                            throw readBackMismatch(record, msg, tx.txKey.txId)
                                .also { tx.pending?.completeExceptionally(it) }

                        applyResolvedTx(tx)
                    }
                }
            }

            is ReplicaMessage.TriesAdded -> {
                if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch)
                    trieCatalog.addTries(msg.tries, record.logTimestamp)

                // Below the guard, not above it: the compactor awaits this watermark and then
                // recalculates jobs off the catalog, so notifying first would have it re-select the
                // job it has just published.
                watchers.notifyApplied(msg.sourceMsgId)
            }

            is BlockBoundary -> {
                // Produce only: the catalog refresh, the index roll, the source watermark and the
                // resolution resume all wait for the `BlockUploaded` this appends to come back.
                blockCutter.upload(PendingBlock(record.msgId, msg))
            }

            // Unreachable: this term's own upload is matched against the block it holds, a lower term's
            // is fenced, and a higher term's resigns the term before it gets here.
            is ReplicaMessage.BlockUploaded ->
                error(
                    "[$dbName] BlockUploaded b${msg.blockIndex.asLexHex} at ${record.msgId} " +
                            "reached apply with no block in flight"
                )

            is ReplicaMessage.NoOp -> watchers.notifyApplied(msg.srcMsgId)

            is ReplicaMessage.TriesDeleted -> gc.triesDeleted(msg)

            is ReplicaMessage.OversizedMessage -> error(
                "[$dbName] OversizedMessage at ${record.msgId} reached apply unresolved (payload=${msg.path})"
            )
        }
    }

    // ---- resolution ----

    /**
     * Run the term until it ends, fail everything staged on it, and raise the fault that stopped it.
     *
     * A resignation is not among those faults: it arrives as a cancellation of this term's job, from the
     * replica tail that read the superseding record.
     */
    suspend fun runTerm(replicaMsgs: ReceiveChannel<ReplicaApply>, afterSourceMessageId: MessageId) {
        coroutineScope {
            try {
                launch { gc.runGc() }
                launch(CoroutineName("$dbName-replica-appender")) { replicaAppender.run() }
                launch(CoroutineName("$dbName-source-tail")) { sourceLog.tailAll(afterSourceMessageId, srcLogProc) }

                extSrcProc?.let { extSrcProc -> launch(CoroutineName("$dbName-ext-source")) { extSrcProc.run() } }

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
            } finally {
                txResolver.cancel()
                srcLogProc.cancel()
            }
        }
    }

    override fun close() {
        txResolver.close()
    }
}
