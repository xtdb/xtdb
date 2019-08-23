package crux.api;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import clojure.lang.Keyword;

/**
 * Provides API access to Crux ingestion.
 */
public interface ICruxIngestAPI extends Closeable {
    /**
     * Writes transactions to the log for processing.
     *
     * @param txOps the transactions to be processed.
     * @return      a map with details about the submitted transaction.
     */
    public Map<Keyword,?> submitTx(List<List<?>> txOps);

    /**
     * Returns a new transaction log context allowing for lazy reading
     * of the transaction log in a try-with-resources block using
     * {@link #txLog(Closeable txLogContext, Long fromTxId, boolean
     * withDocuments)}.
     *
     * @return an implementation specific context.
     */
    public Closeable newTxLogContext();

    /**
     * Reads the transaction log lazily. Optionally includes
     * operations, which allow the contents under the :crux.api/tx-ops
     * key to be piped into {@link #submitTx(List txOps)} of another
     * Crux instance.
     *
     * @param txLogContext     a context from {@link #newTxLogContext()}.
     * @param fromTxId         optional transaction id to start from.
     * @param withOps          should the operations with documents be included?
     * @return                 a lazy sequence of the transaction log.
     */
    public Iterable<List<?>> txLog(Closeable txLogContext, Long fromTxId, boolean withOps);
}
