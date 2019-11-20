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

    @SuppressWarnings("unchecked")
    public TxResult submitTx(TransactionOperation... ops) {
        return submitTx(Arrays.asList(ops));
    }

    public Database db() {
        return database(node);
    }

    public Database db(Date validTime) {
        return database(node, validTime);
    }

    public Database db(Date validTime, Date transactionTime) {
        return database(node, validTime, transactionTime);
    }

    private Document document(Object contentHash) {
        Map<Keyword, Object> doc = node.document(contentHash);
        return Document.document(doc);
    }

    public Iterable<Document> history(CruxId id) {
        List<Map<Keyword,Object>> history = node.history(id.toEdn());
        return history.stream()
            .map(this::document)
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
