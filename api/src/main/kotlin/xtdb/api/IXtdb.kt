package xtdb.api

import xtdb.api.query.QueryOptions
import xtdb.api.query.XtqlQuery
import xtdb.api.tx.TxOp
import xtdb.api.tx.TxOptions
import java.util.stream.Stream

@Suppress("OVERLOADS_INTERFACE")
interface IXtdb : AutoCloseable {

    /**
     * Opens an XTQL query - see [XtqlQuery] for more details on XTQL.
     *
     * By default, this method will block indefinitely until the node has indexed the [requested transaction][QueryOptions.afterTx], or
     * you can specify a [timeout][QueryOptions.txTimeout].
     *
     * @param xtql the XTQL query
     * @param opts query options
     * @return the results stream.
     *         This result stream MUST be explicitly closed when no longer required.
     */
    @JvmOverloads
    fun openQuery(xtql: XtqlQuery, opts: QueryOptions = QueryOptions()) : Stream<Map<String, *>>

    /**
     * Opens an SQL query - see the [SQL documentation](https://docs.xtdb.com/reference/main/sql/queries) for more details on XTDB's SQL support.
     *
     * By default, this method will block indefinitely until the node has indexed the [requested transaction][QueryOptions.afterTx], or
     * you can specify a [timeout][QueryOptions.txTimeout].
     *
     * @see openQuery
     * @param sql the SQL query
     * @param opts query options
     * @return the results stream.
     *         This result stream MUST be explicitly closed when no longer required.
     */
    @JvmOverloads
    fun openQuery(sql: String, opts: QueryOptions = QueryOptions()) : Stream<Map<String, *>>

    /**
     * Submits transactions to the log for processing - this method will block
     * until the log has confirmed receipt of the transaction.
     *
     * @param txOpts options for the transaction
     * @param ops XTQL/SQL transaction operations.
     * @return the transaction key of the submitted transaction.
     */
    fun submitTx(txOpts: TxOptions, vararg ops: TxOp): TransactionKey

    /**
     * Submits transactions to the log for processing - this method will block
     * until the log has confirmed receipt of the transaction.
     *
     * @param ops XTQL/SQL transaction operations.
     * @return the transaction key of the submitted transaction.
     */
    fun submitTx(vararg ops: TxOp) = submitTx(TxOptions(), *ops)

    /**
     * Executes the transaction - this method will block until the receiving node has indexed the transaction.
     *
     * @param txOpts options for the transaction
     * @param ops XTQL/SQL transaction operations.
     * @return the result of the executed transaction.
     */
    fun executeTx(txOpts: TxOptions, vararg ops: TxOp): TransactionResult

    /**
     * Executes the transaction - this method will block until the receiving node has indexed the transaction.
     *
     * @param ops XTQL/SQL transaction operations.
     * @return the result of the executed transaction.
     */
    fun executeTx(vararg ops: TxOp) = executeTx(TxOptions(), *ops)

    /**
     * @suppress
     */
    override fun close()
}
