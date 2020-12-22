package crux.api.transaction;

import clojure.lang.Keyword;
import clojure.lang.LazySeq;

import java.util.Date;
import java.util.Map;

public class TransactionWrapper {
    private static final Keyword idKey = Keyword.intern("crux.tx/tx-id");
    private static final Keyword timeKey = Keyword.intern("crux.tx/tx-time");
    private static final Keyword opsKey = Keyword.intern("crux.api/tx-ops");

    public static TransactionWrapper factory(Map<Keyword,?> data) {
        long id = (long) data.get(idKey);
        Date time = (Date) data.get(timeKey);
        LazySeq transactionRaw = (LazySeq) data.get(opsKey);
        Transaction transaction = Transaction.factory(transactionRaw.toArray());
        return new TransactionWrapper(id, time, transaction);
    }

    private final long id;
    private final Date time;
    private final Transaction transaction;

    private TransactionWrapper(long id, Date time, Transaction transaction) {
        this.id = id;
        this.time = time;
        this.transaction = transaction;
    }

    public long getId() {
        return id;
    }

    public Date getTime() {
        return time;
    }

    public Transaction getTransaction() {
        return transaction;
    }
}
