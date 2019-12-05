package crux.api.alpha;

import clojure.lang.Keyword;
import clojure.lang.LazySeq;
import clojure.lang.PersistentVector;
import crux.api.ICruxAPI;
import crux.api.ICruxIngestAPI;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import static crux.api.alpha.TxResult.txResult;

abstract class CruxIngest {
    protected final ICruxIngestAPI node;

    CruxIngest(ICruxIngestAPI node) {
        this.node = node;
    }

    /**
     * Submits a set of operations to a Crux node
     * @param ops The set of operations to transact
     * @return Returns a TxResult object, containing a transaction Id and transaction time
     * @see TxResult
     */
    @SuppressWarnings("unchecked")
    public TxResult submitTx(Iterable<TransactionOperation> ops) {
        PersistentVector txVector = PersistentVector.create();
        for (TransactionOperation op : ops) {
            txVector = txVector.cons(op.toEdn());
        }

        Map<Keyword,Object> result = node.submitTx(txVector);
        return txResult(result);
    }

    /**
     * Submits a set of operations to a Crux node
     * @param ops The set of operations to transact
     * @return Returns a TxResult object, containing a transaction Id and transaction time
     * @see TxResult
     */
    public TxResult submitTx(TransactionOperation... ops) {
        return submitTx(Arrays.asList(ops));
    }

    /**
     * Returns a new transaction log context allowing for lazy reading
     * of the transaction log in a try-with-resources block using
     * {@link #txLog(Closeable txLogContext, Long fromTxId, boolean
     * withDocuments)}.
     *
     * @return an implementation specific context.
     */
    public Closeable txLogContext() {
        return node.newTxLogContext();
    }

    /**
     * Reads the transaction log lazily. Optionally includes
     * operations, which allow the contents under the :crux.api/tx-ops
     * key to be piped into {@link #submitTx(Iterable ops)} of another
     * Crux instance.
     *
     * @param txLogContext     a context from {@link #txLogContext()}.
     * @param fromTxId         optional transaction id to start from.
     * @param withDocuments    should the operations with documents be included?
     * @return                 a lazy sequence of the transaction log.
     */
    @SuppressWarnings("unchecked")
    public Iterator<TxLog> txLog(Closeable txLogContext, Long fromTxId, boolean withDocuments) {
        LazySeq txLog = (LazySeq) node.txLog(txLogContext, fromTxId, withDocuments);
        return txLog.stream()
            .map(log -> TxLog.txLog((Map<Keyword, Object>) log, withDocuments))
            .iterator();
    }

}
