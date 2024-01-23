package xtdb.api

import xtdb.api.tx.TxOp
import xtdb.api.tx.TxOptions
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException

interface IXtdbSubmitClient {
    private companion object {
        private fun <T> await(fut: CompletableFuture<T>): T {
            try {
                return fut.get()
            } catch (e: InterruptedException) {
                throw RuntimeException(e)
            } catch (e: ExecutionException) {
                throw RuntimeException(e.cause)
            }
        }
    }

    /**
     * Asynchronously submits transactions to the log for processing - this method will return immediately
     * without waiting for the log to confirm receipt of the transaction.
     *
     * @param txOpts options for the transaction
     * @param ops XTQL/SQL transaction operations.
     * @return a [CompletableFuture] eventually containing the transaction key of the submitted transaction.
     */
    fun submitTxAsync(txOpts: TxOptions, vararg ops: TxOp): CompletableFuture<TransactionKey>

    /**
     * Asynchronously submits transactions to the log for processing - this method will return immediately
     * without waiting for the log to confirm receipt of the transaction.
     *
     * @param ops XTQL/SQL transaction operations.
     * @return a [CompletableFuture] eventually containing the transaction key of the submitted transaction.
     */
    fun submitTxAsync(vararg ops: TxOp) = submitTxAsync(TxOptions(), *ops)

    /**
     * Synchronously submits transactions to the log for processing - this method will block
     * until the log has confirmed receipt of the transaction.
     *
     * @param txOpts options for the transaction
     * @param ops XTQL/SQL transaction operations.
     * @return the transaction key of the submitted transaction.
     */
    fun submitTx(txOpts: TxOptions, vararg ops: TxOp) = await(submitTxAsync(txOpts, *ops))

    /**
     * Synchronously submits transactions to the log for processing - this method will block
     * until the log has confirmed receipt of the transaction.
     *
     * @param ops XTQL/SQL transaction operations.
     * @return the transaction key of the submitted transaction.
     */
    fun submitTx(vararg ops: TxOp) = submitTx(TxOptions(), *ops)

    /**
     * @suppress
     */
    fun close()
}
