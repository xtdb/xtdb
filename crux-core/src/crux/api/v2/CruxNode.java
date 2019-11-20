package crux.api.v2;

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

import static crux.api.v2.Database.database;
import static crux.api.v2.TxResult.txResult;
import static crux.api.v2.Util.kw;

public class CruxNode implements AutoCloseable {
    private static final Keyword TX_TIME = kw("crux.tx/tx-time");
    private static final Keyword TX_ID = kw("crux.tx/tx-id");

    private final ICruxAPI node;

    CruxNode(ICruxAPI node) {
        this.node = node;
    }

    @SuppressWarnings("unchecked")
    public TxResult submitTx(Iterable<Operation> ops) {
        PersistentVector txVector = PersistentVector.create();
        for(Operation op: ops) {
            txVector = txVector.cons(op.toEdn());
        }

        Map<Keyword,Object> result = node.submitTx(txVector);
        Date txTime = (Date) result.get(TX_TIME);
        long txId = (Long) result.get(TX_ID);
        return txResult(txTime, txId);
    }

    @SuppressWarnings("unchecked")
    public TxResult submitTx(Operation... ops) { return submitTx(Arrays.asList(ops)); }

    public Database db() {
        return database(node);
    }

    public Database db(Date validTime) {
        return database(node, validTime);
    }

    public Database db(Date validTime, Date transactionTime) {
        return database(node, validTime, transactionTime);
    }

    public Document document(Object contentHash) {
        Map<Keyword,?> doc = node.document(contentHash);
        return Document.document(doc);
    }

    public List<Document> history(CruxId id) {
        List<Map<Keyword,?>> history = node.history(id.toEdn());
        return history.stream()
            .map(Document::document)
            .collect(Collectors.toList());
    }

    public Date sync(Duration timeout) {
        return node.sync(timeout);
    }

    @Override
    public void close() throws IOException {
        node.close();
    }
}
