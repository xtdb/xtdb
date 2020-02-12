package crux.api;

import java.util.List;
import java.util.Map;
import clojure.lang.Keyword;

public interface IQueryAPI {

    /**
     * Queries the db.
     *
     * @param query the query in map, vector or string form.
     * @return      an iterable of result tuples.
     */
    public Iterable<List<?>> query(Object query);

    /**
     * Returns the document map for an entity.
     *
     * @param eid an object that can be coerced into an entity id.
     * @return    the entity document map.
     */
    public Map<Keyword, Object> entity(Object eid);

    /**
     * Returns the transaction details for an entity. Details include tx-id and
     * tx-time.
     *
     * @param eid an object that can be coerced into an entity id.
     * @return    the entity transaction details.
     */
    public Map<Keyword, Object> entityTx(Object eid);

    /**
     * Retrieves entity history in chronological order from and including the
     * valid time of the db while respecting transaction time. Includes the
     * documents.
     *
     * @param eid an object that can be coerced into an entity id.
     * @return    a sequence of history.
     */
    public Iterable<Map<Keyword, Object>> historyAscending(Object eid);

    /**
     * Retrieves entity history in reverse chronological order from and
     * including the valid time of the db while respecting transaction time.
     * Includes the documents.
     *
     * @param eid an object that can be coerced into an entity id.
     * @return    a sequence of history.
     */
    public Iterable<Map<Keyword, Object>> historyDescending(Object eid);
}
