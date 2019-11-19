package crux.api.v2;

import clojure.lang.Keyword;
import clojure.lang.PersistentVector;
import crux.api.Crux;
import crux.api.ICruxAPI;
import crux.api.IndexVersionOutOfSyncException;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static crux.api.v2.Database.database;
import static crux.api.v2.TxResult.txResult;

public class CruxNode {
    private final ICruxAPI node;
    private boolean nodeClosed;

    private CruxNode(Topology topology) throws IndexVersionOutOfSyncException {
        this.node = Crux.startNode(topology.toEdn());
        this.nodeClosed = false;
    }

    private void checkNodeClosed() throws IllegalStateException {
        if(nodeClosed)
            throw new IllegalStateException("Crux node is closed");
    }
    public static CruxNode cruxNode(Topology topology) throws IndexVersionOutOfSyncException, IllegalStateException {
        return new CruxNode(topology);
    }

    @SuppressWarnings("unchecked")
    public TxResult submitTx(Iterable<Operation> ops) throws IllegalStateException{
        checkNodeClosed();
        PersistentVector txVector = PersistentVector.create();
        for(Operation op: ops) {
            txVector = txVector.cons(op.toEdn());
        }

        Map<Keyword,Object> result = node.submitTx(txVector);
        Date txTime = (Date) result.get(Keyword.intern("crux.tx/tx-time"));
        Long txId = (Long) result.get(Keyword.intern("crux.tx/tx-id"));
        return txResult(txTime, txId);
    }

    @SuppressWarnings("unchecked")
    public TxResult submitTx(Operation... ops) throws IllegalStateException { return submitTx(Arrays.asList(ops)); }

    public Database db() throws IllegalStateException {
        checkNodeClosed();
        return database(node);
    }

    public Database db(Date validTime) throws IllegalStateException {
        checkNodeClosed();
        return database(node, validTime);
    }

    public Database db(Date validTime, Date transactionTime) throws IllegalStateException {
        checkNodeClosed();
        return database(node, validTime, transactionTime);
    }

    public Document document(Object contentHash) throws IllegalStateException {
        checkNodeClosed();
        Map<Keyword,?> doc = node.document(contentHash);
        return Document.document(doc);
    }

    public List<Document> history(CruxId id) throws IllegalStateException {
        checkNodeClosed();
        List<Map<Keyword,?>> history = node.history(id.toEdn());
        return history.stream()
            .map(historyMap -> Document.document(historyMap))
            .collect(Collectors.toList());
    }

    public Date sync(Duration timeout) throws IllegalStateException {
        checkNodeClosed();
        return node.sync(timeout);
    }

    public void close() throws IOException {
        checkNodeClosed();
        node.close();
        this.nodeClosed = true;
    }
}
