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

    fun submitTxAsync(txOpts: TxOptions, vararg ops: TxOp): CompletableFuture<TransactionKey>
    fun submitTxAsync(vararg ops: TxOp) = submitTxAsync(TxOptions(), *ops)
    fun submitTx(txOpts: TxOptions, vararg ops: TxOp) = await(submitTxAsync(txOpts, *ops))
    fun submitTx(vararg ops: TxOp) = submitTx(TxOptions(), *ops)

    fun close()
}
