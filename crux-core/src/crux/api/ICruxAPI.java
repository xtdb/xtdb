package crux.api;

import java.io.Closeable;
import java.util.*;
import java.time.Duration;
import java.util.function.Consumer;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

/**
 *  Provides API access to Crux.
 */
public interface ICruxAPI extends ICruxIngestAPI, Closeable {

    /**
     * Returns a db as of now. Will return the latest consistent snapshot of the
     * db currently known. Does not block.
     */
    public ICruxDatasource db();

    /**
     * Returns a db as of now. Will return the latest consistent snapshot of the
     * db currently known. Does not block.
     *
     * This method returns a DB that opens resources shared between method calls
     * - it must be `.close`d when you've finished using it.
     */
    public ICruxDatasource openDB();

    /**
     * Returns a db as of the provided valid time. Will return the latest
     * consistent snapshot of the db currently known, but does not wait for
     * valid time to be current. Does not block.
     */
    public ICruxDatasource db(Date validTime);

    /**
     * Returns a db as of the provided valid time. Will return the latest
     * consistent snapshot of the db currently known, but does not wait for
     * valid time to be current. Does not block.
     *
     * This method returns a DB that opens resources shared between method calls
     * - it must be `.close`d when you've finished using it.
     */
    public ICruxDatasource openDB(Date validTime);

    /**
     * Returns a db as of valid time and transaction time.
     *
     * @throws NodeOutOfSyncException if the node hasn't indexed up to the given `transactionTime`
     */
    public ICruxDatasource db(Date validTime, Date transactionTime) throws NodeOutOfSyncException;

    /**
     * Returns a db as of valid time and transaction time.
     *
     * This method returns a DB that opens resources shared between method calls
     * - it must be `.close`d when you've finished using it.
     *
     * @throws NodeOutOfSyncException if the node hasn't indexed up to the given `transactionTime`
     */
    public ICruxDatasource openDB(Date validTime, Date transactionTime) throws NodeOutOfSyncException;

    /**
     * Returns the status of this node as a map.
     *
     * @return the status map.
     */
    public Map<Keyword,?> status();

    /**
     * Checks if a submitted tx was successfully committed.
     *
     * @param submittedTx must be a map returned from {@link
     * #submitTx(List txOps)}.
     * @return true if the submitted transaction was committed, false if it was not committed.
     * @throws NodeOutOfSyncException if the node has not yet indexed the transaction.
     */
    public boolean hasTxCommitted(Map<Keyword,?> submittedTx) throws NodeOutOfSyncException;

    /**
     * Blocks until the node has caught up indexing to the latest tx available
     * at the time this method is called. Will throw an exception on timeout.
     * The returned date is the latest transaction time indexed by this node.
     * This can be used as the second parameter in {@link #db(Date validTime, Date transactionTime)}
     * for consistent reads.
     *
     * @param timeout max time to wait, can be null for the default.
     * @return the latest known transaction time.
     */
    public Date sync(Duration timeout);

    /**
     * Blocks until the node has indexed a transaction that is past the supplied
     * txTime. Will throw on timeout. The returned date is the latest index time
     * when this node has caught up as of this call.
     *
     * @param txTime transaction time to await.
     * @param timeout max time to wait, can be null for the default.
     * @return the latest known transaction time.
     */
    public Date awaitTxTime(Date txTime, Duration timeout);

    /**
     * Blocks until the node has indexed a transaction that is at or past the
     * supplied tx. Will throw on timeout. Returns the most recent tx indexed by
     * the node.
     *
     * @param tx Transaction to await, as returned from submitTx.
     * @param timeout max time to wait, can be null for the default.
     * @return the latest known transaction.
     */
    public Map<Keyword, ?> awaitTx(Map<Keyword,?> tx, Duration timeout);

    /**
     * Temporary helper value to pass to `listen`, to subscribe to tx-indexed events.
     */
    @SuppressWarnings("unchecked")
    public static final Map<Keyword, ?> TX_INDEXED_EVENT_OPTS = (Map<Keyword, Object>) PersistentArrayMap.EMPTY
        .assoc(Keyword.intern("crux/event-type"), Keyword.intern("crux/tx-indexed"))
        .assoc(Keyword.intern("with-tx-ops?"), true);

    /**
     * Attaches a listener to Crux's event bus.
     *
     * We currently only support one public event-type: `:crux/indexed-tx`.
     * Supplying `:with-tx-ops? true` will include the transaction's operations in the event passed to `f`.
     * See/use {@link #TX_INDEXED_EVENT_OPTS TX_INDEXED_EVENT_OPTS}
     *
     * This is an experimental API, subject to change.
     *
     * @param eventOpts should contain `:crux/event-type`, along with any other options the event-type requires.
     * @return an AutoCloseable - closing the return value detaches the listener.
     */
    public AutoCloseable listen(Map<Keyword, ?> eventOpts, Consumer<Map<Keyword, ?>> listener);

    /**
       @return the latest transaction to have been indexed by this node.
     */
    public Map<Keyword, ?> latestCompletedTx();

    /**
       @return the latest transaction to have been submitted to this cluster
    */
    public Map<Keyword, ?> latestSubmittedTx();

    /**
     * Return frequencies of indexed attributes.
     *
     * @return         Map containing attribute freqencies.
     */
    public Map<Keyword, Long> attributeStats();

    /**
     * Returns a list of currently running queries.
     *
     * @return List containing maps with query information.
     */
    public List<QueryState> activeQueries();

    /**
     * Returns a list of recently completed/failed queries
     *
     * @return List containing maps with query information.
     */
    public List<QueryState> recentQueries();

    /**
     * Returns a list of slowest completed/failed queries ran on the node
     *
     * @return List containing maps with query information.
     */
    public List<QueryState> slowestQueries();
}
