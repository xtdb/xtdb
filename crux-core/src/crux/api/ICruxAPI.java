package crux.api;

import java.io.Closeable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.time.Duration;
import java.util.Set;
import clojure.lang.Keyword;

/**
 *  Provides API access to Crux.
 */
public interface ICruxAPI extends ICruxIngestAPI, Closeable {
    /**
     * Returns a db as of now. Will return the latest consistent
     * snapshot of the db currently known. Does not block.
     *
     * @return the database.
     */
    public ICruxDatasource db();

    /**
     * Returns a db as of valid time. Will return the latest
     * consistent snapshot of the db currently known, but does not
     * wait for valid time to be current. Does not block.
     *
     * @param validTime    the valid time.
     * @return             the database.
     */
    public ICruxDatasource db(Date validTime);

    /**
     * Returns a db as of valid time and transaction time. Will
     * block until the transaction time is present in the index.
     *
     * @param validTime       the valid time.
     * @param transactionTime the transaction time.
     * @return                the database.
     */
    public ICruxDatasource db(Date validTime, Date transactionTime);

    /**
     *  Reads a document from the document store based on its
     *  content hash.
     *
     * @param contentHash an object that can be coerced into a content
     * hash.
     * @return            the document map.
     */
    public Map<Keyword,?> document(Object contentHash);

    /**
     *  Reads a document from the document store based on its
     *  content hash.
     *
     * @param contentHashSet a set of objects that can be coerced into a content
     * hashes.
     * @return            a map from hashable objects to the corresponding documents.
     */
    public Map<String,Map<String,?>> documents(Set<?> contentHashSet);

    /**
     * Returns the transaction history of an entity, in reverse
     * chronological order. Includes corrections, but does not include
     * the actual documents.
     *
     * @param eid an object that can be coerced into an entity id.
     * @return    the transaction history.
     */
    public List<Map<Keyword,?>> history(Object eid);
    // todo elaborate about corrections contents

    /**
     * Returns the transaction history of an entity, ordered by valid
     * time / transaction time in chronological order, earliest
     * first. Includes corrections, but does not include the actual
     * documents.
     *
     * Giving null as any of the date arguments makes the range open
     * ended for that value.
     *
     * @param eid                  an object that can be coerced into an entity id.
     * @param validTimeStart       the start valid time or null.
     * @param transactionTimeStart the start transaction time or null.
     * @param validTimeEnd         the end valid time or null, inclusive.
     * @param transactionTimeEnd   the start transaction time or null, inclusive.
     * @return                     the transaction history.
     */
    public List<Map<Keyword,?>> historyRange(Object eid, Date validTimeStart, Date transactionTimeStart, Date validTimeEnd, Date transactionTimeEnd);
    // todo elaborate

    /**
     * Returns the status of this node as a map.
     *
     * @return the status map.
     */
    public Map<Keyword,?> status();
    // todo elaborate

    /**
     * Checks if a submitted tx did update an entity.
     *
     * @param submittedTx must be a map returned from {@link
     * #submitTx(List txOps)}.
     * @param eid         an object that can be coerced into an entity id.
     * @return            true if the entity was updated in this transaction.
     */
    public boolean hasSubmittedTxUpdatedEntity(Map<Keyword,?> submittedTx, Object eid);

    /**
     * Checks if a submitted tx did correct an entity as of valid
     * time.
     *
     * @param submittedTx  must be a map returned from {@link
     * #submitTx(List txOps)}.
     * @param validTime    valid time of correction to check.
     * @param eid          an object that can be coerced into an entity id.
     * @return             true if the entity was updated in this transaction.
     */
    public boolean hasSubmittedTxCorrectedEntity(Map<Keyword,?> submittedTx, Date validTime, Object eid);

    /**
     * Blocks until the node has caught up indexing. Will throw an
     * exception on timeout. The returned date is the latest index
     * time when this node has caught up as of this call. This can be
     * used as the second parameter in {@link #db(Date validTime,
     * Date transactionTime)} for consistent reads.
     *
     * @param timeout max time to wait, can be null for the default.
     * @return        the latest known transaction time.
     */
    public Date sync(Duration timeout);

    /**
     * Blocks until the node has indexed a transaction that is past
     * the supplied transactionTime. Will throw a timeout. The
     * returned date is the latest index time when this node has
     * caught up as of this call.
     *
     * @param transactionTime transaction time to sync past.
     * @param timeout max time to wait, can be null for the default.
     * @return        the latest known transaction time.
     */
    public Date sync(Date transactionTime, Duration timeout);

    /**
     * Return frequencies of indexed attributes.
     *
     * @return         Map containing attribute freqencies.
     */
    public Map<Keyword, Long> attributeStats();
}
