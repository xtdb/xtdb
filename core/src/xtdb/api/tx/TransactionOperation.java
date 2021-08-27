package xtdb.api.tx;

import clojure.lang.Keyword;

import java.util.List;

public abstract class TransactionOperation {

    public interface Visitor<E> {
        E visit(PutOperation operation);
        E visit(DeleteOperation operation);
        E visit(EvictOperation operation);
        E visit(MatchOperation operation);
        E visit(InvokeFunctionOperation operation);
    }

    public abstract <E> E accept(Visitor<E> visitor);
}
