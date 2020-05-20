package crux.api.alpha;

import clojure.lang.Keyword;
import clojure.lang.PersistentVector;
import crux.api.ICruxAPI;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static crux.api.alpha.Database.database;
import static crux.api.alpha.TxResult.txResult;
import static crux.api.alpha.Util.keyword;

public class CruxNode implements AutoCloseable {
    private static final Keyword TX_TIME = keyword("crux.tx/tx-time");
    private static final Keyword TX_ID = keyword("crux.tx/tx-id");

    private final ICruxAPI node;

    CruxNode(ICruxAPI node) {
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
        Date txTime = (Date) result.get(TX_TIME);
        long txId = (Long) result.get(TX_ID);
        return txResult(txTime, txId);
    }

    /**
     * Submits a set of operations to a Crux node
     * @param ops The set of operations to transact
     * @return Returns a TxResult object, containing a transaction Id and transaction time
     * @see TxResult
     */
    @SuppressWarnings("unchecked")
    public TxResult submitTx(TransactionOperation... ops) {
        return submitTx(Arrays.asList(ops));
    }

    /**
     * Gets a Database instance as of now.
     * @return Database instance at the current time
     * @see Database
     */
    public Database db() {
        return database(node);
    }

    /**
     * Gets a Database instance as of a valid time. Will return the latest consistent snapshot of the db currently known,
     * but does not wait for valid time to be current. Does not block
     * @param validTime The valid time
     * @return Database instance at validTime
     * @see Database
     */
    public Database db(Date validTime) {
        return database(node, validTime);
    }

    /**
     * Gets a Database instance as of a valid and a transaction time. Will block until the transaction time is present in the index.
     * @return Database instance at valid time and transaction time
     * @param validTime The valid time
     * @param transactionTime The transaction time
     * @see Database
     */
    public Database db(Date validTime, Date transactionTime) {
        return database(node, validTime, transactionTime);
    }

    /**
     * Blocks until the node has caught up indexing. Will throw an exception on timeout
     * @param timeout Max time to wait, can be null for the default
     * @return Date representing the latest index time when this node has caught up as of this call
     */
    public Date sync(Duration timeout) {
        return node.sync(timeout);
    }

    @Override
    public void close() throws IOException {
        node.close();
    }
}
