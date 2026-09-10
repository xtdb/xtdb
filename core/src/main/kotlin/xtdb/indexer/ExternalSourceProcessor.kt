package xtdb.indexer

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.SelectBuilder
import xtdb.api.TransactionResult
import xtdb.api.log.Watchers
import xtdb.api.tx.ExternalSource
import xtdb.api.tx.ExternalSourceToken
import xtdb.api.tx.OpenTx
import xtdb.api.tx.TxIndexer
import xtdb.api.tx.TxIndexer.TxResult
import xtdb.catalog.TableCatalog
import java.time.Instant

/**
 * The external source's side of a leader term: the adapter, the queue its transactions arrive on, and the
 * [TxIndexer] it submits them through.
 *
 * The row gauge it feeds and the boundary that gauge may cut are shared with the source log, so ordering
 * between the two is the term's select rather than anything arranged here.
 *
 * [extSource] is borrowed, not owned — it is one-per-database and outlives every term, so nothing here
 * closes it.
 */
internal class ExternalSourceProcessor(
    private val extSource: ExternalSource,
    private val tableCatalog: TableCatalog,
    private val watchers: Watchers,
    private val txResolver: TxResolver,
    private val blockCutter: BlockCutter,
    private val replicaAppender: ReplicaLogAppender,
    private val partition: Int,
    private val leaderTerm: Long
) : TxIndexer {

    override val latestBlock get() = tableCatalog.latestBlock

    private class Task(val msg: ExtSourceMessage) {
        val onComplete = CompletableDeferred<Unit>()

        /** Fail this task's awaiting caller, because the term is going away without finishing it. */
        fun abandon(cause: Throwable) {
            onComplete.completeExceptionally(cause)
            msg.pending.completeExceptionally(cause)
        }
    }

    // capacity 1 so a fire-and-forget `submitTx` caller can queue one tx ahead while the persister works
    // the current one. `executeTx` still blocks on the result regardless of capacity.
    //
    // Undelivered — a cancelled send, or a cancelled channel — is a term-teardown failure like any other,
    // so it goes through the task's own `abandon` rather than a second, hand-rolled policy here.
    private val tasks = Channel<Task>(
        capacity = 1,
        onUndeliveredElement = { it.abandon(CancellationException("leader term closed")) }
    )

    private suspend fun appendTx(resolvedTx: ResolvedTx): Boolean {
        blockCutter.addRows(resolvedTx)

        replicaAppender.append(TxItem(resolvedTx, leaderTerm))

        return blockCutter.isFull
    }

    fun SelectBuilder<Unit>.armSelect() {
        tasks.onReceive { task ->
            try {
                // Verdict discarded: a tx that fills the block cuts it like any other, but there is no
                // batch mid-flight to stop here, so the term's loop cuts it on its next pass.
                appendTx(txResolver.indexTx(task.msg))
                task.onComplete.complete(Unit)
            } catch (e: CancellationException) {
                if (!task.onComplete.isCompleted) task.onComplete.cancel(e)
                throw e
            } catch (e: Throwable) {
                if (!e.isShutdownSignal) {
                    task.msg.pending.let { if (!it.isCompleted) it.completeExceptionally(e) }
                }
                task.onComplete.completeExceptionally(e)
                throw e
            }
        }
    }

    override suspend fun executeTx(
        externalSourceToken: ExternalSourceToken?, systemTime: Instant?,
        writer: suspend (OpenTx) -> TxResult,
    ): TransactionResult =
        submitTx(externalSourceToken, systemTime, writer).await()

    override suspend fun submitTx(
        externalSourceToken: ExternalSourceToken?, systemTime: Instant?,
        writer: suspend (OpenTx) -> TxResult,
    ): Deferred<TransactionResult> {
        val task = Task(ExtSourceMessage(externalSourceToken, systemTime, writer))

        // The send throws if the channel is closed (dead indexer) — the early-exit signal. The returned
        // handle is the message's `pending`, completed on consume-back once the tx is durably replicated
        // AND confirmed unfenced (ReadIndex); an unrecoverable failure also closes the channel with its
        // cause, so the next send throws it.
        tasks.send(task)
        return task.msg.pending
    }

    /** Run the source adapter against this term until cancelled. */
    suspend fun run() {
        try {
            extSource.onPartitionAssigned(partition, watchers.externalSourceToken, this)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            // A supersession reaches here raw, where every other term-teardown cause is re-cast by
            // `asCancellation` and caught above: it says this node has resigned, not that the database
            // has failed, so it is a shutdown signal in everything but type.
            if (!e.isShutdownSignal && e !is LeaderSupersededException) watchers.notifyError(e)
        }
    }

    /**
     * Shut the queue down and fail everyone still waiting on it: senders (via the close cause) and
     * whatever is still queued (via each task's [Task.abandon]).
     *
     * Close and drain are bundled because both are needed and the order matters — `close` alone doesn't
     * visit buffered elements (only `cancel` does), so a queued task's caller would wait forever; and
     * closing *first* means no send can slip into a buffer we've already drained.
     *
     * Only safe on the persister's own exit path: it is the sole receiver, so nothing competes with these
     * `tryReceive`s.
     */
    fun shutdown(cause: Throwable) {
        tasks.close(cause)
        while (true) (tasks.tryReceive().getOrNull() ?: break).abandon(cause)
    }
}
