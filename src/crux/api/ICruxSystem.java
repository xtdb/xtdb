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
     */
    public ICruxDatasource db();

    /**
     * Returns a db as of business time.
     */
    public ICruxDatasource db(Date businessTime);

    /**
     * Returns a db as of business and transaction time time.
     */
    public ICruxDatasource db(Date businessTime, Date transactionTime);

    /**
     *  Reads a document from the document store based on its
     *  content hash.
     */
    public Map document(Object contentHash);

    /**
     * Returns the transaction history of an entity.
     */
    public List<Map> history(Object eid);

    /**
     * Returns the status of this node as a map.
     */
    public Map status();

    /**
     * Writes transactions to the log for processing.
     */
    public Map submitTx(List<List> txOps);

    /**
     * Checks if a submitted tx did update an entity.
     */
    public boolean hasSubmittedTxUpdatedEntity(Map submittedTx, Object eid);
}
