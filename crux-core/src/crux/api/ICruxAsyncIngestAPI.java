package crux.api;

import java.util.List;
import java.util.Map;
import clojure.lang.Keyword;
import clojure.lang.IDeref;

/**
 * Provides API access to Crux async ingestion.
 */
public interface ICruxAsyncIngestAPI extends ICruxIngestAPI {
    /**
     * Writes transactions to the log for processing. Non-blocking.
     *
     * @param txOps the transactions to be processed.
     * @return      a deref with a map with details about the submitted transaction.
     */
    public IDeref submitTxAsync(List<List<?>> txOps);
}
