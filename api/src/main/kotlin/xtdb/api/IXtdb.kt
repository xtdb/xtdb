package xtdb.api

import xtdb.api.query.XtqlQuery
import xtdb.api.query.QueryOptions
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.stream.Stream

@Suppress("OVERLOADS_INTERFACE")
interface IXtdb : IXtdbSubmitClient, AutoCloseable {

    private companion object {
        fun <T> await(fut: CompletableFuture<T>): T {
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
     * Opens an XTQL query - see [XtqlQuery] for more details on XTQL.
     *
     * The [CompletableFuture] will complete with the result stream when the node
     * has indexed the transaction requested in the [query options][QueryOptions.afterTx].
     *
     * @see openQuery
     * @param xtql the XTQL query
     * @param opts query options
     * @return a CompletableFuture containing the results stream.
     *         This result stream MUST be explicitly closed when no longer required.
     */
    @JvmOverloads
    fun openQueryAsync(xtql: XtqlQuery, opts: QueryOptions = QueryOptions()): CompletableFuture<Stream<Map<String, *>>>

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
    fun openQuery(xtql: XtqlQuery, opts: QueryOptions = QueryOptions()) = await(openQueryAsync(xtql, opts))

    /**
     * Opens an SQL query - see the [SQL documentation](https://docs.xtdb.com/reference/main/sql/queries) for more details on XTDB's SQL support.
     *
     * The [CompletableFuture] will complete with the result stream when the node
     * has indexed the transaction requested in the [query options][QueryOptions.afterTx].
     *
     * @see openQuery // can't link to correct overload in KDoc
     * @param sql the SQL query
     * @param opts query options
     * @return a CompletableFuture containing the results stream.
     *         This result stream MUST be explicitly closed when no longer required.
     */
    @JvmOverloads
    fun openQueryAsync(sql: String, opts: QueryOptions = QueryOptions()): CompletableFuture<Stream<Map<String, *>>>

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
    fun openQuery(sql: String, opts: QueryOptions = QueryOptions()) = await(openQueryAsync(sql, opts))
}

fun IXtdb.openQueryAsync(sql: String, configure: QueryOptions.() -> Unit) =
    openQueryAsync(sql, QueryOptions().also { it.configure() })

fun IXtdb.openQuery(sql: String, configure: QueryOptions.() -> Unit) =
    openQuery(sql, QueryOptions().also { it.configure() })

fun IXtdb.openQueryAsync(q: XtqlQuery, configure: QueryOptions.() -> Unit) =
    openQueryAsync(q, QueryOptions().also { it.configure() })

fun IXtdb.openQuery(q: XtqlQuery, configure: QueryOptions.() -> Unit) =
    openQuery(q, QueryOptions().also { it.configure() })
