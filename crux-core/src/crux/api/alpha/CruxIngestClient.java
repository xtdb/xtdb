package crux.api.alpha;

import clojure.lang.IDeref;
import clojure.lang.Keyword;
import clojure.lang.PersistentVector;
import crux.api.ICruxAsyncIngestAPI;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static crux.api.alpha.TxResult.txResult;

public class CruxIngestClient extends CruxIngest implements AutoCloseable {
    private ICruxAsyncIngestAPI ingestAPI;

    CruxIngestClient(ICruxAsyncIngestAPI iCruxAsyncIngestAPI) {
        super(iCruxAsyncIngestAPI);
        this.ingestAPI = iCruxAsyncIngestAPI;
    }

    /**
     * Writes transactions to the log for processing. Non-blocking.
     *
     * @param ops The set of operations to transact
     * @return Returns an AsyncTxResult object, containing transaction Id and transaction time
     * @see AsyncTxResult
     */
    @SuppressWarnings("unchecked")
    public AsyncTxResult submitTxAsync(TransactionOperation... ops) {
        return submitTxAsync(Arrays.asList(ops));
    }

    /**
     * Writes transactions to the log for processing. Non-blocking.
     *
     * @param ops The set of operations to transact
     * @return Returns an AsyncTxResult object, containing transaction Id and transaction time
     * @see AsyncTxResult
     */
    @SuppressWarnings("unchecked")
    public AsyncTxResult submitTxAsync(Iterable<TransactionOperation> ops) {
        PersistentVector txVector = PersistentVector.create();
        for (TransactionOperation op : ops) {
            txVector = txVector.cons(op.toEdn());
        }

        IDeref result = ingestAPI.submitTxAsync(txVector);
        return AsyncTxResult.asyncTxResult(result);
    }

    @Override
    public void close() throws IOException {
        node.close();
    }
}
