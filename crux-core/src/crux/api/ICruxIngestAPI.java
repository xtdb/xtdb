package crux.api;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import clojure.lang.Keyword;
import java.util.stream.Stream;

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
     * Reads the transaction log. Optionally includes  operations, which allow the contents
     * under the :crux.api/tx-ops key to be piped into (submit-tx tx-ops) of another
     * Crux instance.
     *
     * @param fromTxId optional transaction id to start from.
     * @param withOps  should the operations with documents be included?
     * @return         a stream of the transaction log.
     */

    public Stream<?> openTxLog(Long fromTxId, boolean withOps);
}
