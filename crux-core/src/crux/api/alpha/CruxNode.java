package crux.api.alpha;

import clojure.lang.Keyword;
import clojure.lang.LazySeq;
import clojure.lang.PersistentVector;
import crux.api.Crux;
import crux.api.ICruxAPI;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static crux.api.alpha.Database.database;
import static crux.api.alpha.TxResult.txResult;

public class CruxNode implements AutoCloseable {
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
        return txResult(result);
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

    private Document document(Object contentHash) {
        Map<Keyword, Object> doc = node.document(contentHash);
        return Document.document(doc);
    }

    /**
     * Returns the transaction history of an entity, in reverse chronological order. Includes corrections, but does not include the actual documents.
     * @param id Id of the entity to get the history for
     * @return Iterable set of EntityTx's containing transaction information
     * @see EntityTx
     */
    public Iterable<EntityTx> history(CruxId id) {
        List<Map<Keyword,Object>> history = node.history(id.toEdn());
        return history.stream()
            .map(entity -> EntityTx.entityTx(entity))
            .collect(Collectors.toList());
    }

    /**
     * Checks if a submitted tx did update an entity.
     *
     * @param submittedTx TxResult from a submitTx operation.
     * @param eid         CruxId of entity to check.
     * @return            True if the entity was updated in this transaction.
     */
    public boolean hasSubmittedTxUpdatedEntity(TxResult submittedTx, CruxId eid) {
        return node.hasSubmittedTxUpdatedEntity(submittedTx.toEdn(), eid.toEdn());
    }

    /**
     * Checks if a submitted tx did correct an entity as of valid
     * time.
     *
     * @param submittedTx  TxResult from a submitTx operation.
     * @param validTime    Valid time of correction to check.
     * @param eid          CruxId of entity to check.
     * @return             true if the entity was updated in this transaction.
     */
    public boolean hasSubmittedTxCorrectedEntity(TxResult submittedTx, Date validTime, CruxId eid) {
        return node.hasSubmittedTxCorrectedEntity(submittedTx.toEdn(), validTime, eid.toEdn());
    }

    /**
     * Blocks until the node has caught up indexing. Will throw an exception on timeout
     * @param timeout Max time to wait, can be null for the default
     * @return Date representing the latest index time when this node has caught up as of this call
     */
    public Date sync(Duration timeout) {
        return node.sync(timeout);
    }

    /**
     * Blocks until the node has indexed a transaction that is past
     * the supplied transactionTime. Will throw a timeout. The
     * returned date is the latest index time when this node has
     * caught up as of this call.
     *
     * @param transactionTime Transaction time to sync past.
     * @param timeout Max time to wait, can be null for the default.
     * @return Date of the latest known transaction time.
     */
    public Date sync(Date transactionTime, Duration timeout) { return node.sync(transactionTime, timeout);}

    /**
     * Returns status information for the node
     * @return A NodeStatus object containing status information for the node
     * @see NodeStatus
     */
    public NodeStatus status() {
        Map<Keyword,?> status = node.status();
        return NodeStatus.nodeStatus(status);
    }

    public Closeable txLogContext() {
        return node.newTxLogContext();
    }

    @SuppressWarnings("unchecked")
    public Iterator<TxLog> txLog(Closeable txLogContext, Long fromTxId, boolean withDocuments) {
        LazySeq txLog = (LazySeq) node.txLog(txLogContext, fromTxId, withDocuments);
        return txLog.stream()
            .map(log -> TxLog.txLog((Map<Keyword, Object>) log, withDocuments))
            .iterator();
    }


    @Override
    public void close() throws IOException {
        node.close();
    }
}
