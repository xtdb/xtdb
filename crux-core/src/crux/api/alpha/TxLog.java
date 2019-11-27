package crux.api.alpha;

import clojure.lang.Keyword;
import clojure.lang.PersistentVector;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TxLog {
    public final long txId;
    public final Date txTime;
    public final List<TransactionOperation> txOps;

    @Override
    public String toString() {
        return "TxLog{" +
            "txId=" + txId +
            ", txTime=" + txTime +
            ", txOps=" + txOps +
            '}';
    }

    private TxLog(long txId, Date txTime, List<TransactionOperation> txOps) {
        this.txId = txId;
        this.txTime = txTime;
        this.txOps = txOps;
    }

    @SuppressWarnings("unchecked")
    static TxLog txLog(Map<Keyword, Object> log, boolean withDocs) {
        Long txId = (Long) log.get(Util.keyword("crux.tx/tx-id"));
        Date txTime = (Date) log.get(Util.keyword("crux.tx/tx-time"));
        List<TransactionOperation> txOps = null;
        if(withDocs) {
            List<PersistentVector> transaction = (List<PersistentVector>) log.get(Util.keyword("crux.api/tx-ops"));
            txOps = transaction.stream()
                .map(TransactionOperation::fromEdn)
                .collect(Collectors.toList());
        }

        return new TxLog(txId, txTime, txOps);
    }
}
