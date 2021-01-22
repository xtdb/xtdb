package crux.api.tx;

import clojure.lang.IPersistentVector;
import clojure.lang.Keyword;

public abstract class TransactionOperation {
    enum Type {
        PUT("crux.tx/put"),
        DELETE("crux.tx/delete"),
        EVICT("crux.tx/evict"),
        MATCH("crux.tx/match"),
        FN("crux.tx/fn");

        private final Keyword keyword;

        Type(String idRaw) {
            keyword = Keyword.intern(idRaw);
        }

        final Keyword getKeyword() {
            return keyword;
        }
    }

    public interface Visitor {
        void visit(PutOperation operation);
        void visit(DeleteOperation operation);
        void visit(EvictOperation operation);
        void visit(MatchOperation operation);
        void visit(FunctionOperation operation);
    }

    public abstract IPersistentVector toVector();
    public abstract Type getType();
    public abstract void accept(Visitor visitor);
}
