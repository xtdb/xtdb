@file:OptIn(xtdb.InternalApi::class)

package xtdb.indexer

import io.micrometer.core.instrument.Counter
import kotlinx.coroutines.CompletableDeferred
import org.apache.arrow.memory.BufferAllocator
import xtdb.Metrics.withSpan
import xtdb.NodeBase
import xtdb.api.DatabaseName
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.TransactionResult.Aborted
import xtdb.api.TransactionResult.Committed
import xtdb.api.error.Anomaly
import xtdb.api.error.Fault
import xtdb.api.error.Incorrect
import xtdb.api.log.DbOp
import xtdb.api.log.SourceMessage
import xtdb.api.tx.ExternalSourceToken
import xtdb.api.tx.OpenTx
import xtdb.api.tx.TxIndexer.TxResult
import xtdb.arrow.Relation
import xtdb.arrow.VectorReader
import xtdb.arrow.asChannel
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.time.InstantUtil.asMicros
import xtdb.time.InstantUtil.fromMicros
import xtdb.tx.deserializeUserMetadata
import xtdb.types.MessageId
import xtdb.util.StringUtil.asLexDec
import xtdb.util.asPath
import xtdb.util.closeAll
import xtdb.util.debug
import xtdb.util.logger
import xtdb.util.warn
import java.nio.ByteBuffer
import java.time.Instant
import java.time.InstantSource
import java.time.ZoneId
import java.time.ZonedDateTime

private val SKIPPED_EXN: Throwable = Fault("Transaction was skipped", "xtdb/skipped-tx")

private val LOG = TxResolver::class.logger

/**
 * An external source's request to index a transaction: the [writer] that stages its ops, alongside the
 * [pending] handle its submitter awaits. The leader completes [pending] with the tx's result once the tx has
 * been read back off the replica log and applied, or fails it on any path where that will never happen
 * (writer throw, append fault, term cancellation, undelivered send).
 */
internal class ExtSourceMessage(
    val externalSourceToken: ExternalSourceToken?,
    val systemTime: Instant?,
    val writer: suspend (OpenTx) -> TxResult,
) {
    val pending = CompletableDeferred<TransactionResult>()
}

/**
 * Resolves transactions for the leader: turns a source-log message, a database attach/detach, or an
 * external source's writer into a [ResolvedTx], and holds it until the leader has applied it.
 *
 * State, no processes — the [LeaderLogProcessor] both creates and drives this, calling it only from its
 * persister coroutine, so the resolver is single-threaded by confinement and needs no lock.
 *
 * ## The resolved-tx queue
 *
 * Every resolved tx joins the tail of a FIFO and stays there until the leader has read it back off its own
 * replica log and imported it into the durable live index, at which point it leaves via [removeHead]. The
 * queue OWNS its txs — their table relations — and frees them at [removeHead] (once imported) or [close]
 * (teardown). The leader's append pump only borrows a tx to serialize it ([ResolvedTx.toReplicaMessage]);
 * it never closes one.
 *
 * Everything still queued is a read-your-writes predecessor for the next tx to resolve, which is why the
 * queue and resolution sit together: a tx resolving now must see every predecessor not yet applied.
 *
 * Two heads: [latestCompletedTx] here is the RESOLVED head (it drives resolution — the next external-source
 * tx-id and system-time smoothing), and it leads [LiveIndex.latestCompletedTx] (the durable/query basis) by
 * everything queued but not yet applied.
 */
internal class TxResolver(
    allocator: BufferAllocator,
    private val nodeBase: NodeBase,
    private val partitionStorage: PartitionStorage,
    private val partitionState: PartitionState,
    private val dbName: DatabaseName,
    crashLogger: CrashLogger,
    private val skipTxs: Set<MessageId>,
    private val instantSource: InstantSource,
) : AutoCloseable {

    private val bufferPool = partitionStorage.bufferPool

    private val allocator = allocator.newChildAllocator("tx-resolver", 0, Long.MAX_VALUE)

    private val tracer = nodeBase.tracer?.takeIf { nodeBase.config.tracer.transactionTracing }

    private val txErrorCounter: Counter? = nodeBase.meterRegistry?.let { Counter.builder("tx.error").register(it) }

    private val sourceLogTxIndexer = SourceLogTxIndexer(this.allocator, nodeBase, partitionState, dbName, crashLogger)

    var latestCompletedTx: TransactionKey? = partitionState.liveIndex.latestCompletedTx
        private set

    private val queue = ArrayDeque<ResolvedTx>()

    /** Queued predecessors for resolution layering (read-your-writes), oldest→newest. */
    private val resolvedTxs: List<ResolvedTx> get() = queue.toList()

    /** Remove and return the head (oldest) tx; ownership passes to the caller, which imports then closes it. */
    fun removeHead(): ResolvedTx = queue.removeFirst()

    /**
     * Fail every pending deferred we're still holding. Called on teardown paths where queued txs will never
     * settle — the leader's persister is exiting and nobody else will complete them.
     */
    fun failPending(cause: Throwable) = queue.forEach { it.pending?.completeExceptionally(cause) }

    /**
     * Take ownership of [openTx]'s written tables (a reference move — see `OpenTx.sealTables`), enqueue them,
     * and advance the resolved head. The caller closes the (now table-less) [openTx].
     */
    private fun stage(
        openTx: OpenTx, srcMsgId: MessageId, txResult: TransactionResult,
        dbOp: DbOp?, pending: CompletableDeferred<TransactionResult>?,
    ): ResolvedTx =
        ResolvedTx.stage(openTx, srcMsgId, txResult, dbOp, pending)
            .also { queue.addLast(it); latestCompletedTx = openTx.txKey }

    private fun openTx(txKey: TransactionKey, externalSourceToken: ExternalSourceToken?) =
        OpenTx(allocator, nodeBase, partitionStorage, partitionState, dbName, txKey, externalSourceToken, tracer, resolvedTxs)

    // Stage a fresh tx committing a single skip / abort / invalid-system-time row. `countError` mirrors the
    // pre-staging behaviour: skipped txs don't count as errors.
    private fun stageStandaloneTx(
        msgId: MessageId, txKey: TransactionKey, error: Throwable, userMetadata: Map<*, *>?, countError: Boolean,
    ): ResolvedTx {
        if (countError) txErrorCounter?.increment()

        return openTx(txKey, null).use { openTx ->
            openTx.writeTxRow(error, userMetadata)
            stage(openTx, msgId, Aborted(txKey, error), dbOp = null, pending = null)
        }
    }

    private fun indexSourceLogTx(
        msgId: MessageId,
        msgTimestamp: Instant,
        txOps: VectorReader?,
        systemTime: Instant?,
        defaultTz: ZoneId?,
        user: String?,
        userMetadata: Any?,
    ): ResolvedTx = tracer.withSpan(
        "xtdb.transaction",
        attributes = mapOf("operations.count" to (txOps?.valueCount ?: 0).toString()),
    ) {
        val userMetadataMap = userMetadata as? Map<*, *>
        // The RESOLVED head (the queue), not the durable head: a tx must system-time-smooth against the
        // queued predecessors it resolves behind, which lead the durable index.
        val lcTx = latestCompletedTx

        // If lc-tx's systemTime >= msgTimestamp, bump past it by 1µs; otherwise use msgTimestamp.
        // (`+1000ns` is `+1µs`.)
        val defaultSystemTime: Instant = lcTx?.systemTime?.let { lcSysTime ->
            if (lcSysTime >= msgTimestamp) lcSysTime.plusNanos(1_000) else null
        } ?: msgTimestamp

        // Specified system-time before lc-tx → invalid; abort with that error.
        // The aborted tx-key uses the *default* (smoothed) systemTime, not the rejected one,
        // so the tx-key still satisfies the monotonicity invariant.
        if (systemTime != null && lcTx != null && systemTime < lcTx.systemTime) {
            val err = Incorrect(
                "specified system-time older than current tx",
                "invalid-system-time",
                mapOf(
                    "tx-key" to TransactionKey(msgId, systemTime),
                    "latest-completed-tx" to lcTx,
                ),
            )
            LOG.warn { "specified system-time '$systemTime' older than current tx '$lcTx'" }

            return@withSpan stageStandaloneTx(
                msgId, TransactionKey(msgId, defaultSystemTime), err, userMetadataMap, countError = true
            )
        }

        val effectiveSystemTime = systemTime ?: defaultSystemTime
        val txKey = TransactionKey(msgId, effectiveSystemTime)

        if (txOps == null)
            return@withSpan stageStandaloneTx(msgId, txKey, SKIPPED_EXN, userMetadataMap, countError = false)

        val openTx = openTx(txKey, null)
        val result = try {
            val opts = SourceLogTxIndexer.TxOpts(
                txKey = txKey,
                currentTime = effectiveSystemTime,
                systemTime = effectiveSystemTime.asMicros,
                defaultTz = defaultTz,
                user = user,
            )
            sourceLogTxIndexer.ForTx(txOps, opts).indexTx(openTx)
        } catch (e: Throwable) {
            openTx.close(); throw e
        }

        when (result) {
            is TxResult.Committed ->
                openTx.use {
                    it.writeTxRow(null, userMetadataMap)
                    stage(it, msgId, Committed(txKey), dbOp = null, pending = null)
                }

            is TxResult.Aborted -> {
                LOG.debug(result.error) { "aborted tx" }
                // fresh tx for the abort row — the original openTx may hold partial writes
                openTx.close()
                stageStandaloneTx(msgId, txKey, result.error, userMetadataMap, countError = true)
            }
        }
    }

    // Park the payload for later inspection, then stage the tx as a no-op abort so the tx-id sequence
    // (and everything downstream keyed on it) stays unbroken.
    private fun indexSkippedTx(msgId: MessageId, msgTimestamp: Instant, payload: ByteArray): ResolvedTx {
        LOG.warn("[$dbName] Skipping transaction id $msgId - within XTDB_SKIP_TXS")

        bufferPool.putObjectSync("skipped-txs/${msgId.asLexDec}".asPath, ByteBuffer.wrap(payload))

        return indexSourceLogTx(msgId, msgTimestamp, null, null, null, null, null)
    }

    fun indexTx(msgId: MessageId, msgTimestamp: Instant, msg: SourceMessage.Tx): ResolvedTx {
        if (msgId in skipTxs) return indexSkippedTx(msgId, msgTimestamp, msg.encode())

        return msg.txOps.asChannel.use { ch ->
            Relation.StreamLoader(allocator, ch).use { loader ->
                Relation(allocator, loader.schema).use { rel ->
                    loader.loadNextPage(rel)

                    val userMetadata = msg.userMetadata?.let { deserializeUserMetadata(allocator, it) }

                    indexSourceLogTx(
                        msgId, msgTimestamp,
                        rel["tx-ops"],
                        msg.systemTime, msg.defaultTz, msg.user, userMetadata
                    )
                }
            }
        }
    }

    fun indexTx(msgId: MessageId, msgTimestamp: Instant, msg: SourceMessage.LegacyTx): ResolvedTx {
        if (msgId in skipTxs) return indexSkippedTx(msgId, msgTimestamp, msg.payload)

        return msg.payload.asChannel.use { txOpsCh ->
            Relation.StreamLoader(allocator, txOpsCh).use { loader ->
                Relation(allocator, loader.schema).use { rel ->
                    loader.loadNextPage(rel)

                    val systemTime = (rel["system-time"].getObject(0) as ZonedDateTime?)?.toInstant()
                    val defaultTz = (rel["default-tz"].getObject(0) as String?).let { ZoneId.of(it) }
                    val userMetadata = rel.vectorForOrNull("user-metadata")?.getObject(0)
                    val user = rel.vectorForOrNull("user")?.getObject(0) as String?

                    indexSourceLogTx(
                        msgId, msgTimestamp,
                        rel["tx-ops"].listElements,
                        systemTime, defaultTz, user, userMetadata
                    )
                }
            }
        }
    }

    private fun smoothSystemTime(systemTime: Instant): Instant {
        val lct = latestCompletedTx?.systemTime ?: return systemTime
        val floor = fromMicros(lct.asMicros + 1)
        return if (systemTime.isBefore(floor)) floor else systemTime
    }

    /**
     * Run an external source's writer and stage the tx it produces.
     *
     * [srcMsgId] is the source-log watermark to stamp on the replicated record. Ext-source txs carry no
     * source-log position of their own — they track progress via `externalSourceToken` — but without the
     * stamp a follower's `latestSourceMsgId` lags between block boundaries, and on promotion it resumes the
     * source log from a stale point and replays an already-covered block boundary.
     */
    suspend fun indexTx(msg: ExtSourceMessage, srcMsgId: MessageId): ResolvedTx {
        val txKey = TransactionKey(
            (latestCompletedTx?.txId ?: -1) + 1,
            smoothSystemTime(msg.systemTime ?: instantSource.instant())
        )

        var openTx = openTx(txKey, msg.externalSourceToken)

        @Suppress("ConvertTryFinallyToUseCall") // because openTx is a var
        try {
            return try {
                val writerResult = msg.writer(openTx)
                val txResult: TransactionResult = when (writerResult) {
                    is TxResult.Committed -> {
                        openTx.writeTxRow(null, writerResult.userMetadata)
                        Committed(txKey)
                    }

                    is TxResult.Aborted -> {
                        txErrorCounter?.increment()
                        openTx.close()
                        // fresh tx for the abort row — the original openTx may hold partial writes
                        openTx = openTx(txKey, msg.externalSourceToken)
                        openTx.writeTxRow(writerResult.error, writerResult.userMetadata)
                        Aborted(txKey, writerResult.error)
                    }
                }

                stage(openTx, srcMsgId, txResult, dbOp = null, pending = msg.pending)
            } catch (e: Throwable) {
                // Writer, writeTxRow, or stage threw before the tx reached the queue, so nothing will ever
                // settle it — complete the handle here, or a caller awaiting it hangs until the term closes.
                msg.pending.completeExceptionally(e)
                throw e
            }
        } finally {
            openTx.close()
        }
    }

    /** Stage the tx row recording an applied database attach/detach. */
    fun indexDbOp(msgId: MessageId, msgTimestamp: Instant, dbOp: DbOp): ResolvedTx {
        val txKey = TransactionKey(msgId, msgTimestamp)

        return openTx(txKey, null).use { openTx ->
            openTx.writeTxRow(null, null)
            stage(openTx, msgId, Committed(txKey), dbOp, pending = null)
        }
    }

    /** Stage the abort row for a database attach/detach the catalog rejected. */
    fun indexFailedDbOp(msgId: MessageId, msgTimestamp: Instant, error: Anomaly.Caller): ResolvedTx {
        val txKey = TransactionKey(msgId, msgTimestamp)

        return openTx(txKey, null).use { openTx ->
            openTx.writeTxRow(error, null)
            stage(openTx, msgId, Aborted(txKey, error), dbOp = null, pending = null)
        }
    }

    // Called in the leader's close, once its persister is joined so nothing live still touches the slices.
    override fun close() {
        queue.closeAll()
        allocator.close() // last: Arrow won't close it while a child buffer is live
    }
}
