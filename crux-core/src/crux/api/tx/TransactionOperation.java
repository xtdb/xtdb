package crux.api.tx;

import clojure.lang.IPersistentVector;
import clojure.lang.Keyword;

public abstract class TransactionOperation {
    enum Type {
        PUT("crux.tx/put"),
        DELETE("crux.tx/delete"),
        EVICT("crux.tx/evict"),
        MATCH("crux.tx/match");

        private final Keyword keyword;

        Type(String idRaw) {
            keyword = Keyword.intern(idRaw);
        }

        final Keyword getKeyword() {
            return keyword;
        }
    }

    public abstract IPersistentVector toVector();
    public abstract Type getType();
}
