package crux.api;

import java.io.Closeable;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import clojure.lang.Keyword;

// TODO document
public interface ICruxQueryAPI {
    /**
     * Returns the document map for an entity.
     *
     * @param eid an object that can be coerced into an entity id.
     * @return    the entity document map.
     */
    public Map<Keyword, ?> entity(Object eid);

    /**
     * Returns the transaction details for an entity. Details
     * include tx-id and tx-time.
     *
     * @param eid an object that can be coerced into an entity id.
     * @return    the entity transaction details.
     */
    public Map<Keyword, ?> entityTx(Object eid);

    /**
     * Queries the db.
     *
     * @param query the query in map, vector or string form.
     * @return      a set or vector of result tuples.
     */
    public Iterable<List<?>> q(Object query);

    /**
     * Retrieves entity history in chronological order from and
     * including the valid time of the db while respecting
     * transaction time. Includes the documents.
     *
     * @param eid      an object that can be coerced into an entity id.
     * @return         history entries.
     */
    public Iterable<Map<Keyword,?>> historyAscending(Object eid);

    /**
     * Retrieves entity history in reverse chronological order
     * from and including the valid time of the db while respecting
     * transaction time. Includes the documents.
     *
     * @param eid      an object that can be coerced into an entity id.
     * @return         history entries.
     */
    public Iterable<Map<Keyword,?>> historyDescending(Object eid);

    public IBitemporalInstant instant();
}
