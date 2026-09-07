package xtdb.indexer

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
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
import xtdb.api.tx.ExternalSource
import xtdb.database.Database
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.types.MessageId
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.logger
import xtdb.util.useAll
import java.time.Duration
import java.time.InstantSource

private val LOG = LeaderLogProcessor::class.logger

/**
 * A higher-term record read back on our own replica log: a newer leader has superseded us. Thrown from
 * the apply loop to fail the term cleanly (not a query-facing fault, so it doesn't poison the watchers);
 * the transport re-follows on the next rebalance. See #5817.
 */
internal class LeaderSupersededException(message: String) : RuntimeException(message)

internal class LeaderLogProcessor(
    private val al: BufferAllocator,
    nodeBase: NodeBase,
    partitionStorage: PartitionStorage,
    crashLogger: CrashLogger,
    partitionState: PartitionState,
    private val dbName: DatabaseName,
    private val driver: LeaderDriver,
    private val blockCutter: BlockCutter,
    private val watchers: Watchers,

    private val replicaAppender: ReplicaLogAppender,

    extSource: ExternalSource?,
    skipTxs: Set<MessageId>,
    private val dbCatalog: Database.Catalog?,
    val leaderTerm: Long = 0,
    instantSource: InstantSource = InstantSource.system(),
    flushTimeout: Duration,
    // Base for the GCs' delete fan-out; defaults to IO in prod, sims inject the seeded dispatcher.
    gcDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : AutoCloseable {

    init {
        require((dbCatalog != null) == (dbName == "xtdb")) {
            "dbCatalog must be provided iff database is 'xtdb'"
        }
    }

    private val partition = partitionStorage.partition

    private val tableCatalog = partitionState.tableCatalog

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
        nodeBase, partitionStorage, partitionState, dbName, leaderTerm, replicaAppender, gcDispatcher
    )

    val srcLogProc = SourceLogProcessor(
        driver, txResolver, partitionStorage, partitionState, watchers, dbCatalog, dbName, leaderTerm,
        replicaAppender, flushTimeout, ::appendTx, ::cutBlock
    )

    // An ext-source tx that fills a block cuts it like any other, but there is no batch mid-flight to
    // stop, so the processor is handed the append alone.
    val extSrcProc =
        extSource?.let { source ->
            ExternalSourceProcessor(source, partition, tableCatalog, watchers, txResolver) { appendTx(it) }
        }

    private suspend fun applyResolvedTx(record: Log.Record<ReplicaMessage>, msg: ReplicaMessage.ResolvedTx) {
        val txKey = TransactionKey(msg.txId, msg.systemTime)

        msg.loadTableData(al).useAll { tables -> driver.applyTx(txKey, tables) }

        val result =
            if (msg.committed) TransactionResult.Committed(txKey)
            else TransactionResult.Aborted(txKey, msg.error)

        // Handling for pre-`f3eb8d7d9` ResolvedTx records — see #5586.
        val effectiveSrcMsgId = msg.srcMsgId
            ?: if (extSrcProc != null) watchers.latestSourceMsgId else msg.txId

        watchers.notifyApplied(
            record.msgId, srcMsgId = effectiveSrcMsgId, result, msg.externalSourceToken
        )
    }

    private suspend fun applyResolvedTx(record: Log.Record<ReplicaMessage>, tx: ResolvedTx) {
        try {
            driver.applyTx(tx.txKey, tx.allTables.associate { it.ref to it.relation })

            watchers.notifyApplied(record.msgId, tx.srcMsgId, tx.txResult, tx.externalSourceToken)

            tx.pending?.complete(tx.txResult)
        } catch (e: Throwable) {
            // tx is already off the queue, so teardown's failPending can't reach it — this catch is the
            // only thing that will ever fail its handle.
            tx.pending?.completeExceptionally(e)
            throw e
        }
    }

    suspend fun applyReplicaMessage(record: Log.Record<ReplicaMessage>) {
        val msg = record.message

        when (msg) {
            is ReplicaMessage.ResolvedTx -> {
                // Ahead of the tx itself, so a caller that submitted an attach can use the database as
                // soon as its transaction returns — and once, whichever way the tx below is applied.
                // Nothing to guard on `committed`: a refused dbOp resolves to an abort carrying no dbOp.
                applyDbOp(msg.dbOp)

                txResolver.removeHead(msg.txId).use { tx ->
                    if (tx != null) {
                        applyResolvedTx(record, tx)
                    } else {
                        applyResolvedTx(record, msg)
                    }
                }
            }

            // Catalog already updated on the resolve side; here we only advance the source watermark.
            is ReplicaMessage.TriesAdded -> watchers.notifyApplied(record.msgId, msg.sourceMsgId)

            is BlockBoundary -> {
                blockCutter.upload(record.msgId, msg)

                // the block's covered source position, as the follower does
                watchers.notifyApplied(record.msgId, msg.latestProcessedMsgId)

                gc.signal()

                srcLogProc.blockUploaded()
            }

            // Our own BlockUploaded, read back after uploadBlock already rolled the index — nothing to do
            // but advance the watermark.
            is ReplicaMessage.BlockUploaded -> watchers.notifyApplied(record.msgId, msg.latestProcessedMsgId)

            is ReplicaMessage.NoOp -> watchers.notifyApplied(record.msgId, msg.srcMsgId)

            // Catalog already updated on the resolve side (see GarbageCollector.handleTask); nothing to do.
            is ReplicaMessage.TriesDeleted -> watchers.notifyApplied(record.msgId)
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

    // ---- resolution ----

    private suspend fun cutBlock(latestProcessedMsgId: MessageId) =
        blockCutter.cut(latestProcessedMsgId, txResolver.resolvedExtToken)

    // Hand a freshly-resolved tx to the append pump, answering whether it filled the block — which the
    // caller needs, because a source batch mid-flight has to stop where that happens.
    private suspend fun appendTx(resolvedTx: ResolvedTx): Boolean {
        blockCutter.addRows(resolvedTx.allTables.sumOf { it.relation.rowCount.toLong() })

        replicaAppender.append(TxItem(resolvedTx, leaderTerm))

        return blockCutter.isFull
    }

    val acceptingResolution get() = blockCutter.acceptingResolution

    val blockFilled get() = blockCutter.isFull

    /** Cut the block this term has filled, sealing it at the resolve side's current watermarks. */
    suspend fun cutFilledBlock() = cutBlock(txResolver.resolvedSrcMsgId)

    /**
     * Fail everything staged on this term, because it has ended with [cause].
     *
     * Nothing may be left awaiting the term once it has gone: whatever is staged, paused, or still queued
     * gets failed here. Each task's own `abandon` picks the failure *kind*, so this is a flat sweep with no
     * per-caller special-casing.
     *
     * Miss anything and the symptom is a hang, not an error — and for a source-log batch that hang is on
     * the transport's poll thread (inside `processRecords`), which is also the sole servicer of the
     * transport's unregister. So it wedges the whole subscription teardown and blows
     * `DatabaseCatalog.close`'s bound (#5711 / #5817).
     */
    fun shutdown(cause: Throwable) {
        txResolver.failPending(cause)
        srcLogProc.shutdown(cause)
        extSrcProc?.shutdown(cause)
        gc.shutdown(cause)

        replicaAppender.shutdown(cause)
    }

    override fun close() {
        // Frees every resolved-but-not-applied tx — safe only once the term's job has been joined, so the
        // persister and the pumps are gone.
        txResolver.close()
    }
}
