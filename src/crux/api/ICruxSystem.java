package crux.api;

import java.io.Closeable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.time.Duration;

/**
 *  Provides API access to Crux.
 */
public interface ICruxSystem extends Closeable {
    /**
     * Returns a db as of now. Will return the latest consistent
     * snapshot of the db currently known. Does not block.
     *
     * @return the database.
     */
    public ICruxDatasource db();

    /**
     * Returns a db as of business time. Will return the latest
     * consistent snapshot of the db currently known, but does not
     * wait for business time to be current. Does not block.
     *
     * @param businessTime the business time.
     * @return             the database.
     */
    public ICruxDatasource db(Date businessTime);

    /**
     * Returns a db as of business and transaction time time. Will
     * block until the transaction time is present in the index.
     *
     * @param businessTime    the business time.
     * @param transactionTime the transaction time.
     * @return                the database.
     */
    public ICruxDatasource db(Date businessTime, Date transactionTime);

    /**
     *  Reads a document from the document store based on its
     *  content hash.
     *
     * @param contentHash an object that can be coerced into a content
     * hash.
     * @return            the document map.
     */
    public Map document(Object contentHash);

    /**
     * Returns the transaction history of an entity, in reverse
     * chronological order.
     *
     * @param eid an object that can be coerced into an entity id.
     * @return    the transaction history.
     */
    public List<Map> history(Object eid);

    /**
     * Returns the status of this node as a map.
     *
     * @return the status map.
     */
    public Map status();

    /**
     * Writes transactions to the log for processing.
     *
     * @param txOps the transactions to be processed.
     * @return      a map with details about the submitted transaction.
     */
    public Map submitTx(List<List> txOps);

    /**
     * Checks if a submitted tx did update an entity.
     *
     * @param submittedTx must be a map returned from {@link
     * #submitTx(List txOps)}.
     * @param eid         an object that can be coerced into an entity id.
     * @return            true if the entity was updated in this transaction.
     */
    public boolean hasSubmittedTxUpdatedEntity(Map submittedTx, Object eid);


    /**
     * Returns a new transaction log context allowing for lazy reading
     * of the transaction log in a try-with-resources block using
     * {@link #txLog(Closeable txLogContext)}.
     *
     * @return an implementation specific context.
     */
    public Closeable newTxLogContext();

    /**
     * Blocks until the node has caught up indexing. Will throw an
     * exception on timeout. The returned date is the latest index
     * time when this node has caught up as of this call. This can be
     * used as the second parameter in {@link #db(Date businessTime,
     * Date transactionTime)} for consistent reads.
     *
     * @param timeout max time to wait, can be null for the default.
     * @return        the latest known transaction time.
     */
    public Date sync(Duration timeout);

    /**
     * Reads the transaction log lazily.
     *
     * @param txLogContext a context from {@link #newTxLogContext()}.
     * @return             a lazy sequence of the transaction log.
     */
    public Iterable<List> txLog(Closeable txLogContext);
}
