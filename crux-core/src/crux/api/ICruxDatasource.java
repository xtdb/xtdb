package crux.api;

import java.io.Closeable;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import clojure.lang.Keyword;

/**
 * Represents the database as of a specific valid and
 * transaction time.
 */
public interface ICruxDatasource extends Closeable {

    public ICruxDatasource openReadTx();

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
     * @param query the query in map, vector or string form.
     * @return      a collection of result tuples.
     */
    public Collection<List<?>> q(Object query);

    /**
     * Queries the db.
     *
     * @param query the query in map, vector or string form.
     * @return      a stream of result tuples.
     */
    public Stream<List<?>> openQ(Object query);

    /**
     * Retrieves entity history lazily in chronological order from and
     * including the valid time of the db while respecting
     * transaction time. Includes the documents.
     *
     * @param eid an object that can be coerced into an entity id.
     * @return    a stream of history.
     */
    public Stream<Map<Keyword,?>> openHistoryAscending(Object eid);

    /**
     * Retrieves entity history lazily in reverse chronological order
     * from and including the valid time of the db while respecting
     * transaction time. Includes the documents.
     *
     * @param eid an object that can be coerced into an entity id.
     * @return    a stream of history.
     */
    public Stream<Map<Keyword,?>> openHistoryDescending(Object eid);

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
}
