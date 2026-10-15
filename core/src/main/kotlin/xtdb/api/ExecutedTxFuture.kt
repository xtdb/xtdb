package xtdb.api

import xtdb.api.Xtdb.ExecutedTx
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

internal fun TransactionResult.toExecutedTx() =
    ExecutedTx(
        txKey,
        this is TransactionResult.Committed,
        (this as? TransactionResult.Aborted)?.error
    )

/**
 * A submitted transaction's outcome: the [result] the watchers hand back,
 * or - where they had moved past the transaction before it was awaited, and hand back null - what [lookUp] reads,
 * on the thread that reads this future.
 */
internal class ExecutedTxFuture(
    private val result: Future<TransactionResult?>,
    private val lookUp: () -> ExecutedTx,
) : Future<ExecutedTx> {

    private fun resolve(res: TransactionResult?): ExecutedTx =
        res?.toExecutedTx()
            ?: try {
                lookUp()
            } catch (e: Exception) {
                throw ExecutionException(e)
            }

    override fun get(): ExecutedTx = resolve(result.get())
    override fun get(timeout: Long, unit: TimeUnit): ExecutedTx = resolve(result.get(timeout, unit))
    override fun isDone(): Boolean = result.isDone
    override fun isCancelled(): Boolean = result.isCancelled
    override fun cancel(mayInterruptIfRunning: Boolean): Boolean = result.cancel(mayInterruptIfRunning)
}
