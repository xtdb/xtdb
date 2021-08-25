package crux.api;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import clojure.lang.IDeref;
import crux.api.tx.Transaction;

/**
 * Provides API access to Crux async ingestion.
 */
@SuppressWarnings("unused")
public interface ICruxAsyncIngestAPI extends ICruxIngestAPI {
    /**
     * Writes transactions to the log for processing. Non-blocking.
     *
     * @param txOps the transactions to be processed.
     * @return      a {@link CompletableFuture} with a map with details about the submitted transaction.
     */
    CompletableFuture<TransactionInstant> submitTxAsync(Transaction txOps);

    /**
     * Writes transactions to the log for processing. Non-blocking.
     *
     * @param txOps the transactions to be processed.
     * @return      a deref with a map with details about the submitted transaction.
     * @deprecated in favour of {@link #submitTxAsync(Transaction)}
     */
    @Deprecated
    IDeref submitTxAsync(List<List<?>> txOps);
}
