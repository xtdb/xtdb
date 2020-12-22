package crux.api;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import clojure.lang.Keyword;
import crux.api.transaction.Transaction;
import crux.api.transaction.TransactionWrapper;

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

    default Map<Keyword,?> submit(Consumer<Transaction.Builder> f) {
        return submit(Transaction.build(f));
    }

    default Map<Keyword,?> submit(Transaction transaction) {
        return submitTx(transaction.toEdn());
    }

    /**
     * Reads the transaction log. Optionally includes  operations, which allow the contents
     * under the :crux.api/tx-ops key to be piped into (submit-tx tx-ops) of another
     * Crux instance.
     *
     * @param afterTxId optional transaction id to start after.
     * @param withOps   should the operations with documents be included?
     * @return          a lazy sequence of the transaction log.
     */
    public ICursor<Map<Keyword, ?>> openTxLog(Long afterTxId, boolean withOps);

    default ICursor<TransactionWrapper> openTxLog(Long afterTxId) {
        ICursor<Map<Keyword, ?>> cursor = openTxLog(afterTxId, true);
        return cursor.map(TransactionWrapper::factory);
    }
}
