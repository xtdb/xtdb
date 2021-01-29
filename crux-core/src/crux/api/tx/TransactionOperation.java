package crux.api.tx;

import clojure.lang.Keyword;

import java.util.List;

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

    public interface Visitor<E> {
        E visit(PutOperation operation);
        E visit(DeleteOperation operation);
        E visit(EvictOperation operation);
        E visit(MatchOperation operation);
        E visit(InvokeFunctionOperation operation);
    }

    public abstract Type getType();
    public abstract <E> E accept(Visitor<E> visitor);


}
