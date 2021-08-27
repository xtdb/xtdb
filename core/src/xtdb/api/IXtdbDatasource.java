package xtdb.api;

import java.io.Closeable;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import clojure.lang.Keyword;
import xtdb.api.tx.Transaction;

/**
 * Represents the database as of a specific valid and
 * transaction time.
 */
@SuppressWarnings("unused")
public interface IXtdbDatasource extends Closeable {
    /**
     * Returns the document map for an entity.
     *
     * @param eid an object that can be coerced into an entity id.
     * @return    the entity document map.
     */
    XtdbDocument entity(Object eid);

    /**
     * Returns the transaction details for an entity. Details
     * include tx-id and tx-time.
     *
     * @param eid an object that can be coerced into an entity id.
     * @return    the entity transaction details.
     */
    Map<Keyword,?> entityTx(Object eid);

    /**
     * Queries the db.
     *
     * This function will return a set of result tuples if you do not specify `:order-by`, `:limit` or `:offset`;
     * otherwise, it will return a vector of result tuples.
     *
     * @param query the query in map, vector or string form.
     * @param args  bindings for in.
     * @return      a set or vector of result tuples.
     */
    Collection<List<?>> query(Object query, Object... args);

    /**
     * Queries the db lazily.
     *
     * @param query the query in map, vector or string form.
     * @param args  bindings for in.
     * @return      a cursor of result tuples.
     */
    ICursor<List<?>> openQuery(Object query, Object... args);

    /**
     * Returns the requested data for the given entity ID, based on the projection spec
     *
     * e.g. `db.pull("[:film/name :film/year]", "spectre")`
     *   =&gt; `{:film/name "Spectre", :film/year 2015}`
     *
     * @param projection An EQL projection spec as a String or Clojure data structure - see https://opencrux.com/reference/queries.html#pull
     * @param eid entity ID
     * @return the requested projection starting at the given entity
     */
    Map<Keyword, ?> pull(Object projection, Object eid);

    /**
     * Returns the requested data for the given entity IDs, based on the projection spec
     *
     * e.g. `db.pullMany("[:film/name :film/year]", Arrays.asList("spectre", "skyfall"))`
     *   =&gt; `[{:film/name "Spectre", :film/year 2015}, {:film/name "Skyfall", :film/year 2012}]`
     *
     * @param projection An EQL projection spec as a String or Clojure data structure - see https://opencrux.com/reference/queries.html#pull
     * @param eids entity IDs
     * @return the requested projections starting at the given entities
     */
    List<Map<Keyword, ?>> pullMany(Object projection, Iterable<?> eids);

    /**
     * Returns the requested data for the given entity IDs, based on the projection spec
     *
     * e.g. `db.pullMany("[:film/name :film/year]", "spectre", "skyfall")`
     *   =&gt; `[{:film/name "Spectre", :film/year 2015}, {:film/name "Skyfall", :film/year 2012}]`
     *
     * @param projection An EQL projection spec - see https://opencrux.com/reference/queries.html#pull
     * @param eids entity IDs
     * @return the requested projections starting at the given entities
     */
    List<Map<Keyword, ?>> pullMany(Object projection, Object... eids);

    /**
     * Eagerly retrieves entity history for the given entity.
     *
     * Each entry in the result contains the following keys:
     * * `::xt/valid-time`,
     * * `::xt/tx-time`,
     * * `::xt/tx-id`,
     * * `::xt/content-hash`
     * * `::xt/doc` (if {@link HistoryOptions#withDocs(boolean) withDocs} is set on the options).
     *
     * If {@link HistoryOptions#withCorrections(boolean) withCorrections} is set
     * on the options, bitemporal corrections are also included in the sequence,
     * sorted first by valid-time, then tx-id.
     *
     * No matter what `start` and `end` parameters you specify, you won't receive
     * results later than the valid-time and transact-time of this DB value.
     *
     * @param eid The entity id to return history for
     * @return an eagerly-evaluated sequence of changes to the given entity.
     */
    List<Map<Keyword, ?>> entityHistory(Object eid, HistoryOptions options);

    /**
     * Eagerly retrieves entity history for the given entity.
     * @see #entityHistory(Object, HistoryOptions)
     * @return an eagerly-evaluated sequence of changes to the given entity.
     */
    default List<Map<Keyword, ?>> entityHistory(Object eid, HistoryOptions.SortOrder sortOrder) {
        return entityHistory(eid, HistoryOptions.create(sortOrder));
    }

    /**
     * Lazily retrieves entity history for the given entity.
     * Don't forget to close the cursor when you've consumed enough history!
     *
     * @see #entityHistory(Object, HistoryOptions)
     * @return a cursor of changes to the given entity.
     */
    ICursor<Map<Keyword, ?>> openEntityHistory(Object eid, HistoryOptions options);

    /**
     * Lazily retrieves entity history for the given entity.
     * Don't forget to close the cursor when you've consumed enough history!
     *
     * @see #entityHistory(Object, HistoryOptions)
     * @return a cursor of changes to the given entity.
     */
    default ICursor<Map<Keyword, ?>> openEntityHistory(Object eid, HistoryOptions.SortOrder sortOrder) {
        return openEntityHistory(eid, HistoryOptions.create(sortOrder));
    }

    /**
     * The valid time of this db.
     * If valid time wasn't specified at the moment of the db value retrieval
     * then valid time will be time the db value was retrieved.
     *
     * @return the valid time of this db.
     */
    Date validTime();

    /**
     * @return the time of the latest transaction applied to this db value.
     * If a tx time was specified when db value was acquired then returns
     * the specified time.
     */
    Date transactionTime();

    /**
     * @return the basis of this database snapshot.
     */
    DBBasis dbBasis();

    /**
     * Returns a new db value with the txOps speculatively applied.
     * The txOps will only be visible in the value returned from this method - they're not submitted to the cluster, nor are they visible to any other database value in your application.
     *
     * If the transaction doesn't commit (eg because of a failed 'match'), this function returns null.
     *
     * @param transaction the transaction to be applied.
     * @return a new db value with the transaction speculatively applied.
    */
    IXtdbDatasource withTx(Transaction transaction);

    /**
     * Returns a new db value with the txOps speculatively applied.
     * The txOps will only be visible in the value returned from this method - they're not submitted to the cluster, nor are they visible to any other database value in your application.
     *
     * If the transaction doesn't commit (eg because of a failed 'match'), this function returns null.
     *
     * @param transaction the transaction to be applied.
     * @return a new db value with the transaction speculatively applied.
     * @deprecated in favour of {@link #withTx(Transaction)}
     */
    @Deprecated
    IXtdbDatasource withTx(List<List<?>> transaction);
}
