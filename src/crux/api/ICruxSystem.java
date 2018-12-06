package crux.api;

import java.io.Closeable;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 *  Provides API access to Crux.
 */
public interface ICruxSystem extends Closeable {
    /**
     * Returns a db as of now.
     *
     * @return the database.
     */
    public ICruxDatasource db();

    /**
     * Returns a db as of business time.
     *
     * @param businessTime the business time.
     * @return             the database.
     */
    public ICruxDatasource db(Date businessTime);

    /**
     * Returns a db as of business and transaction time time.
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
     * Returns the transaction history of an entity.
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
     * #submitTx(List<List> txOps)}.
     * @param eid         an object that can be coerced into an entity id.
     * @return            true if the entity was updated in this transaction.
     */
    public boolean hasSubmittedTxUpdatedEntity(Map submittedTx, Object eid);


    /**
     * Returns a new transaction log context allowing for lazy reading
     * of the transaction log in a try-with-resources block using
     * {@link #txLog(Closeable txLogContext)}.
     *
     * @return an implementation specific snapshot
     */
    public Closeable newTxLogContext();

    /**
     * Reads the transaction log lazily.
     *
     * @param snapshot a snapshot from {@link #newSnapshot()]}.
     * @return         a lazy sequence of the transaction log.
     */
    public Iterable<List> txLog(Closeable txLogContext);
}
