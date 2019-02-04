package crux.api;

import java.io.Closeable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents the database as of a specific business and
 * transaction time.
 */
public interface ICruxDatasource {
    /**
     * Returns the document map for an entity.
     *
     * @param eid an object that can be coerced into an entity id.
     * @return    the entity document map.
     */
    public Map entity(Object eid);

    /**
     * Returns the entity tx for an entity.
     *
     * @param eid an object that can be coerced into an entity id.
     * @return    the entity transaction details.
     */
    public Map entityTx(Object eid);

    /**
     * Returns a new snapshot allowing for lazy query results in a
     * try-with-resources block using {@link #q(Closeable snapshot,
     * Object query)}. Can also be used for {@link
     * #historyAscending(Closeable snapshot, Object eid)} and {@link
     * #historyDescending(Closeable snapshot, Object eid)}
     *
     * @return an implementation specific snapshot
     */
    public Closeable newSnapshot();

    /**
     * Queries the db.
     *
     * @param query the query in map, vector or string form.
     * @return      a set of result tuples.
     */
    public Set<List> q(Object query);

    /**
     * Queries the db lazily.
     *
     * @param snapshot a snapshot from {@link #newSnapshot()}.
     * @param query    the query in map, vector or string form.
     * @return         a lazy sequence of result tuples.
     */
    public Iterable<List> q(Closeable snapshot, Object query);

    /**
     * Retrieves entity history lazily in chronological order from and
     * including the business time of the db while respecting
     * transaction time.
     *
     * @param snapshot a snapshot from {@link #newSnapshot()}.
     * @param eid      an object that can be coerced into an entity id.
     * @return         a lazy sequence of history.
     */
    public Iterable<Map> historyAscending(Closeable snapshot, Object eid);

    /**
     * Retrieves entity history lazily in reverse chronological order
     * from and including the business time of the db while respecting
     * transaction time.
     *
     * @param snapshot a snapshot from {@link #newSnapshot()}.
     * @param eid      an object that can be coerced into an entity id.
     * @return         a lazy sequence of history.
     */
    public Iterable<Map> historyDescending(Closeable snapshot, Object eid);

    /**
     * The business time of this db.
     *
     * @return the business time of this db.
     */
    public Date businessTime();

    /**
     * The transaction time of this db.
     *
     * @return the transaction time of this db.
     */
    public Date transactionTime();
}
