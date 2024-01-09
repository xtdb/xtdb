package xtdb.api

import xtdb.query.Query
import xtdb.query.QueryOpts
import xtdb.tx.TxOp
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.stream.Stream

@Suppress("OVERLOADS_INTERFACE")
interface IXtdb : AutoCloseable {
    @JvmOverloads
    fun openQueryAsync(q: Query, opts: QueryOpts = QueryOpts()): CompletableFuture<Stream<Map<String, *>>>

    @JvmOverloads
    fun openQuery(q: Query, opts: QueryOpts = QueryOpts()) = await(openQueryAsync(q, opts))

    fun submitTxAsync(txOpts: TxOptions, vararg ops: TxOp): CompletableFuture<TransactionKey>

    fun submitTxAsync(vararg ops: TxOp) = submitTxAsync(TxOptions(), *ops)

    fun submitTx(txOpts: TxOptions, vararg ops: TxOp) = await(submitTxAsync(txOpts, *ops))

    fun submitTx(vararg ops: TxOp) = submitTx(TxOptions(), *ops)

    override fun close()

    companion object {
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
}
