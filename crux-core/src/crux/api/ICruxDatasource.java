package crux.api;

import java.io.Closeable;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import clojure.lang.Keyword;

/**
 * Represents the database as of a specific valid and
 * transaction time.
 */
public interface ICruxDatasource extends Closeable {
    /**
     * Returns the document map for an entity.
     *
     * @param eid an object that can be coerced into an entity id.
     * @return    the entity document map.
     */
    public Map<Keyword,Object> entity(Object eid);

    /**
     * Returns the transaction details for an entity. Details
     * include tx-id and tx-time.
     *
     * @param eid an object that can be coerced into an entity id.
     * @return    the entity transaction details.
     */
    public Map<Keyword,?> entityTx(Object eid);

    /**
     * Queries the db.
     *
     * This function will return a set of result tuples if you do not specify `:order-by`, `:limit` or `:offset`;
     * otherwise, it will return a vector of result tuples.
     *
     * @param query the query in map, vector or string form.
     * @return      a set or vector of result tuples.
     */
    public Collection<List<?>> query(Object query);

    /**
     * Queries the db lazily.
     *
     * @param query the query in map, vector or string form.
     * @return      a cursor of result tuples.
     */
    public ICursor<List<?>> openQuery(Object query);

    /**
     * Eagerly retrieves entity history for the given entity.
     *
     * Each entry in the result contains the following keys:
     * * `:crux.db/valid-time`,
     * * `:crux.db/tx-time`,
     * * `:crux.tx/tx-id`,
     * * `:crux.db/content-hash`
     * * `:crux.db/doc` (if {@link HistoryOptions#withDocs(boolean) withDocs} is set on the options).
     *
     * If {@link HistoryOptions#withCorrections(boolean) withCorrections} is set
     * on the options, bitemporal corrections are also included in the sequence,
     * sorted first by valid-time, then transaction-time.
     *
     * No matter what `start` and `end` parameters you specify, you won't receive
     * results later than the valid-time and transact-time of this DB value.
     *
     * @param eid The entity id to return history for
     * @return an eagerly-evaluated sequence of changes to the given entity.
     */
    public List<Map<Keyword, ?>> entityHistory(Object eid, HistoryOptions options);

    /**
     * Lazily retrieves entity history for the given entity.
     * Don't forget to close the cursor when you've consumed enough history!
     *
     * @see #entityHistory(Object, HistoryOptions)
     * @param eid The entity id to return history for
     * @return a cursor of changes to the given entity.
     */
    public ICursor<Map<Keyword, ?>> openEntityHistory(Object eid, HistoryOptions options);

    /**
     * The valid time of this db.
     * If valid time wasn't specified at the moment of the db value retrieval
     * then valid time will be time of the latest transaction.
     *
     * @return the valid time of this db.
     */
    public Date validTime();

    /**
     * @return the time of the latest transaction applied to this db value.
     * If a tx time was specified when db value was acquired then returns
     * the specified time.
     */
    public Date transactionTime();

    /**
     * Returns a new db value with the txOps speculatively applied.
     * The txOps will only be visible in the value returned from this method - they're not submitted to the cluster, nor are they visible to any other database value in your application.
     *
     * If the transaction doesn't commit (eg because of a failed 'match'), this function returns nil.
     *
     * @param txOps the transaction operations to be applied.
     * @return a new db value with the txOps speculatively applied.
    */
    public ICruxDatasource withTx(List<List<?>> txOps);
}
