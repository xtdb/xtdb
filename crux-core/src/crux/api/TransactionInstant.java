package crux.api;

import java.lang.Long;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import clojure.lang.IFn;
import clojure.lang.ILookup;
import clojure.lang.Keyword;
import clojure.java.api.Clojure;

public interface TransactionInstant extends ILookup {
    class Builder {
        private static final IFn REQUIRING_RESOLVE =
            Clojure.var("clojure.core", "requiring-resolve");

        static final IFn TO_TRANSACTION_INSTANT =
            (IFn) REQUIRING_RESOLVE.invoke(Clojure.read("crux.transaction-instant/->transaction-instant"));
    }

    public static TransactionInstant create(Long txId) {
        return create(txId, null);
    }

    public static TransactionInstant create(Date txTime) {
        return create(null, txTime);
    }

    public static TransactionInstant create(Long txId, Date txTime) {
	Map<Keyword, Object> txMap = new HashMap<Keyword, Object>();
	txMap.put(Keyword.intern("crux.tx/tx-id"), txId);
	txMap.put(Keyword.intern("crux.tx/tx-time"), txTime);
        return (TransactionInstant) Builder.TO_TRANSACTION_INSTANT.invoke(txMap);
    }
}
